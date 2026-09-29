use core::{future::Future, task::Context};
use std::{sync::Barrier, task::Wake, thread};

use super::*;

const EVENT: EventId = EventId::from_name("tests.grant.event");
const OTHER: EventId = EventId::from_name("tests.grant.other");

fn permission(rights: Rights) -> Permission {
    Permission {
        event: EVENT,
        rights,
        limits: StreamLimits::ZERO,
    }
}

#[test]
fn attenuation_shares_reservations_until_the_last_child_owner_drops() {
    let root = GrantNode::root(Limits {
        streams: 2,
        grants: 1,
        retained_items: 3,
        ..Limits::ZERO
    });
    let child_limits = Limits {
        streams: 1,
        retained_items: 2,
        ..Limits::ZERO
    };
    let child = root
        .restrict(&[permission(Rights::OBSERVE)], child_limits)
        .unwrap();
    assert_eq!(child.check(EVENT, Rights::OBSERVE), Ok(()));
    assert_eq!(child.check(EVENT, Rights::PUBLISH), Err(TopicError::Denied));
    assert_eq!(child.check(OTHER, Rights::OBSERVE), Err(TopicError::Denied));
    assert!(matches!(
        child.restrict(&[permission(Rights::PUBLISH)], child_limits),
        Err(TopicError::Denied)
    ));
    let clone = child.clone();
    drop(child);
    assert_eq!(root.usage().retained_items, 2);
    assert!(matches!(
        root.try_charge(Limits {
            retained_items: 2,
            ..Limits::ZERO
        }),
        Err(TopicError::Capacity)
    ));
    let item = root
        .try_charge(Limits {
            retained_items: 1,
            ..Limits::ZERO
        })
        .unwrap();
    assert_eq!(root.usage().retained_items, 3);
    drop(clone);
    assert_eq!(root.usage().grants, 0);
    assert_eq!(root.usage().streams, 0);
    assert_eq!(root.usage().retained_items, 1);
    drop(item);
    assert_eq!(root.usage(), Limits::ZERO);
}

#[test]
fn empty_grants_still_consume_depth_and_node_allowance() {
    let root = GrantNode::root(Limits {
        grants: 2,
        ..Limits::ZERO
    });
    let child = root
        .restrict(
            &[],
            Limits {
                grants: 1,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let grandchild = child.restrict(&[], Limits::ZERO).unwrap();
    assert!(matches!(
        grandchild.restrict(&[], Limits::ZERO),
        Err(TopicError::Capacity)
    ));
    assert!(matches!(
        root.restrict(&[], Limits::ZERO),
        Err(TopicError::Capacity)
    ));
    drop(child);
    assert_eq!(root.usage().grants, 2);
    drop(grandchild);
    assert_eq!(root.usage().grants, 0);
}

#[test]
fn duplicate_permissions_combine_without_expanding_parent_authority() {
    let root = GrantNode::root(Limits {
        streams: 2,
        grants: 1,
        ..Limits::ZERO
    });
    let child = root
        .restrict(
            &[permission(Rights::PUBLISH), permission(Rights::OBSERVE)],
            Limits {
                streams: 2,
                ..Limits::ZERO
            },
        )
        .unwrap();
    assert_eq!(
        child.check(EVENT, Rights::PUBLISH | Rights::OBSERVE),
        Ok(())
    );
    assert_eq!(
        child.check(EVENT, Rights::REQUIRED),
        Err(TopicError::Denied)
    );
}

#[test]
fn failed_multi_resource_charge_returns_every_partial_reservation() {
    let root = GrantNode::root(Limits {
        retained_items: 1,
        bytes: 3,
        ..Limits::ZERO
    });
    let bytes = root
        .try_charge(Limits {
            bytes: 3,
            ..Limits::ZERO
        })
        .unwrap();
    assert!(matches!(
        root.try_charge(Limits {
            retained_items: 1,
            bytes: 1,
            ..Limits::ZERO
        }),
        Err(TopicError::Capacity)
    ));
    assert_eq!(
        root.usage(),
        Limits {
            bytes: 3,
            ..Limits::ZERO
        }
    );
    let item = root
        .try_charge(Limits {
            retained_items: 1,
            ..Limits::ZERO
        })
        .unwrap();
    drop(bytes);
    drop(item);
    assert_eq!(root.usage(), Limits::ZERO);
    assert_eq!(
        Limits {
            bytes: usize::MAX,
            ..Limits::ZERO
        }
        .validate(),
        Err(TopicError::InvalidConfig)
    );
}

#[test]
fn ledger_matches_exhaustive_small_capacity_sequences() {
    fn explore(node: &Arc<GrantNode>, held: &mut Vec<Lease>, used: usize, depth: usize) {
        assert_eq!(node.usage().retained_items, used);
        if depth == 0 {
            return;
        }
        for amount in 0..=3 {
            let result = node.try_charge(Limits {
                retained_items: amount,
                ..Limits::ZERO
            });
            if used + amount <= 2 {
                held.push(result.unwrap());
                explore(node, held, used + amount, depth - 1);
                drop(held.pop());
            } else {
                assert!(matches!(result, Err(TopicError::Capacity)));
            }
            assert_eq!(node.usage().retained_items, used);
        }
    }
    let root = GrantNode::root(Limits {
        retained_items: 2,
        ..Limits::ZERO
    });
    explore(&root, &mut Vec::new(), 0, 5);
    assert_eq!(root.usage(), Limits::ZERO);
}

#[test]
fn event_and_generic_pools_match_a_conservation_model_under_delegation() {
    enum Held {
        Item(Lease),
        Child(Arc<GrantNode>),
    }
    impl Held {
        fn release(self) {
            match self {
                Self::Item(lease) => drop(lease),
                Self::Child(child) => drop(child),
            }
        }
    }
    fn explore(node: &Arc<GrantNode>, used: [usize; 3], children: usize, depth: usize) {
        assert_eq!(node.usage().retained_items, used.iter().sum());
        assert_eq!(node.usage().grants, children);
        if depth == 0 {
            return;
        }
        for pool in 0..3 {
            for amount in 0..=3 {
                for delegate in [false, true] {
                    let event = [EVENT, OTHER, EVENT][pool];
                    let data = StreamLimits {
                        retained_items: amount,
                        ..StreamLimits::ZERO
                    };
                    let expected =
                        used[pool] + amount <= [2, 2, 1][pool] && (!delegate || children < 2);
                    let result = if delegate {
                        let permissions = [Permission {
                            event,
                            rights: Rights::PUBLISH,
                            limits: data,
                        }];
                        node.restrict(
                            if pool == 2 { &[] } else { &permissions },
                            Limits {
                                streams: 1,
                                retained_items: amount,
                                ..Limits::ZERO
                            },
                        )
                        .map(Held::Child)
                    } else if pool == 2 {
                        node.try_charge(data.as_limits()).map(Held::Item)
                    } else {
                        node.try_charge_stream(event, data).map(Held::Item)
                    };
                    if expected {
                        let held = result.unwrap();
                        let mut next = used;
                        next[pool] += amount;
                        explore(node, next, children + usize::from(delegate), depth - 1);
                        held.release();
                    } else {
                        assert!(matches!(result, Err(TopicError::Capacity)));
                    }
                    assert_eq!(node.usage().retained_items, used.iter().sum());
                    assert_eq!(node.usage().grants, children);
                }
            }
        }
    }
    let root = GrantNode::root(Limits {
        streams: 2,
        grants: 3,
        retained_items: 5,
        ..Limits::ZERO
    });
    let data = StreamLimits {
        retained_items: 2,
        ..StreamLimits::ZERO
    };
    let node = root
        .restrict(
            &[
                permission(Rights::PUBLISH).with_limits(data),
                Permission {
                    event: OTHER,
                    rights: Rights::PUBLISH,
                    limits: data,
                },
            ],
            Limits {
                streams: 2,
                grants: 2,
                retained_items: 5,
                ..Limits::ZERO
            },
        )
        .unwrap();
    explore(&node, [0; 3], 0, 3);
    assert_eq!(node.usage(), Limits::ZERO);
    drop(node);
    assert_eq!(root.usage(), Limits::ZERO);
}

#[test]
fn conflicting_duplicate_partitions_and_oversized_totals_fail_without_reservations() {
    let root = GrantNode::root(Limits {
        streams: 2,
        grants: 1,
        retained_items: 2,
        ..Limits::ZERO
    });
    let single = permission(Rights::PUBLISH).with_limits(StreamLimits {
        retained_items: 1,
        ..StreamLimits::ZERO
    });
    let double = single.with_limits(StreamLimits {
        retained_items: 2,
        ..StreamLimits::ZERO
    });
    let total = Limits {
        streams: 2,
        retained_items: 2,
        ..Limits::ZERO
    };
    assert!(matches!(
        root.restrict(&[single, double], total),
        Err(TopicError::InvalidConfig)
    ));
    assert_eq!(root.usage(), Limits::ZERO);
    assert!(matches!(
        root.restrict(
            &[double],
            Limits {
                retained_items: 1,
                ..total
            }
        ),
        Err(TopicError::Capacity)
    ));
    assert_eq!(root.usage(), Limits::ZERO);
    let child = root.restrict(&[double, double], total).unwrap();
    assert_eq!(root.usage().retained_items, 2);
    let held = child
        .try_charge_stream(
            EVENT,
            StreamLimits {
                retained_items: 2,
                ..StreamLimits::ZERO
            },
        )
        .unwrap();
    assert!(matches!(
        child.try_charge_stream(
            EVENT,
            StreamLimits {
                retained_items: 1,
                ..StreamLimits::ZERO
            }
        ),
        Err(TopicError::Capacity)
    ));
    drop(held);
    drop(child);
    assert_eq!(root.usage(), Limits::ZERO);
}

#[test]
fn concurrent_charges_cannot_oversubscribe_one_item() {
    let root = GrantNode::root(Limits {
        retained_items: 1,
        ..Limits::ZERO
    });
    let barrier = Arc::new(Barrier::new(8));
    let successes = AtomicUsize::new(0);
    thread::scope(|scope| {
        for _ in 0..8 {
            let node = root.clone();
            let barrier = barrier.clone();
            let successes = &successes;
            scope.spawn(move || {
                barrier.wait();
                let lease = node.try_charge(Limits {
                    retained_items: 1,
                    ..Limits::ZERO
                });
                if lease.is_ok() {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
                barrier.wait();
                drop(lease);
            });
        }
    });
    assert_eq!(successes.load(Ordering::Relaxed), 1);
    assert_eq!(root.usage(), Limits::ZERO);
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn revocation_reaches_active_descendants_without_waiting_for_administration() {
    let root = GrantNode::root(Limits {
        streams: 1,
        grants: 1,
        ..Limits::ZERO
    });
    let child = root
        .restrict(
            &[permission(Rights::PUBLISH)],
            Limits {
                streams: 1,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let operation = child.enter(EVENT, Rights::PUBLISH).unwrap();
    let counter = Arc::new(WakeCount::default());
    let waker = counter.clone().into();
    let mut context = Context::from_waker(&waker);
    let mut changed = core::pin::pin!(child.changed.notified());
    assert!(changed.as_mut().poll(&mut context).is_pending());
    let parent_guard = root.administration.lock().unwrap();
    let child_guard = child.administration.lock().unwrap();
    let (completed, completion) = std::sync::mpsc::channel();
    thread::scope(|scope| {
        let root = root.clone();
        let task = scope.spawn(move || {
            let mut revoke = core::pin::pin!(root.revoke());
            completed
                .send(
                    revoke
                        .as_mut()
                        .poll(&mut Context::from_waker(core::task::Waker::noop())),
                )
                .unwrap();
        });
        let result = completion.recv_timeout(std::time::Duration::from_secs(2));
        drop(parent_guard);
        drop(child_guard);
        task.join().unwrap();
        assert!(result
            .expect("revocation waited for an administrative lock")
            .is_pending());
    });
    assert!(child.is_revoked());
    assert!(counter.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        child.check(EVENT, Rights::PUBLISH),
        Err(TopicError::Revoked)
    );
    drop(operation);
    let mut revoke = core::pin::pin!(root.revoke());
    assert_eq!(
        revoke.as_mut().poll(&mut context),
        core::task::Poll::Ready(Ok(()))
    );
}

#[test]
fn revocation_waits_for_descendant_admission_and_does_not_mint_credits() {
    let root = GrantNode::root(Limits {
        streams: 1,
        grants: 1,
        ..Limits::ZERO
    });
    let child = root
        .restrict(
            &[permission(Rights::PUBLISH)],
            Limits {
                streams: 1,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let operation = child.enter(EVENT, Rights::PUBLISH).unwrap();
    let wake = Arc::new(WakeCount::default());
    let waker = wake.clone().into();
    let mut context = Context::from_waker(&waker);
    let mut revoke = core::pin::pin!(root.revoke());
    assert!(revoke.as_mut().poll(&mut context).is_pending());
    assert!(child.is_revoked());
    assert_eq!(
        child.check(EVENT, Rights::PUBLISH),
        Err(TopicError::Revoked)
    );
    assert!(matches!(
        root.restrict(&[], Limits::ZERO),
        Err(TopicError::Revoked)
    ));
    let mut concurrent = core::pin::pin!(root.revoke());
    assert_eq!(
        concurrent.as_mut().poll(&mut context),
        core::task::Poll::Ready(Err(TopicError::Capacity))
    );
    drop(operation);
    assert!(wake.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        revoke.as_mut().poll(&mut context),
        core::task::Poll::Ready(Ok(()))
    );
    assert_eq!(root.usage().grants, 1);
    drop(child);
    assert_eq!(root.usage().grants, 0);
}

#[test]
fn cancelling_revocation_preserves_cutoff_and_releases_its_control_waiter() {
    let root = GrantNode::root(Limits::ZERO);
    let operation = root.enter(EVENT, Rights::PUBLISH).unwrap();
    let mut context = Context::from_waker(core::task::Waker::noop());
    {
        let mut revoke = core::pin::pin!(root.revoke());
        assert!(revoke.as_mut().poll(&mut context).is_pending());
    }
    assert!(root.is_revoked());
    let mut retry = core::pin::pin!(root.revoke());
    assert!(retry.as_mut().poll(&mut context).is_pending());
    drop(operation);
    assert_eq!(
        retry.as_mut().poll(&mut context),
        core::task::Poll::Ready(Ok(()))
    );
}

#[test]
fn released_credits_wake_only_the_matching_stream_and_registration_is_bounded() {
    let allowance = StreamLimits {
        retained_items: 1,
        ..StreamLimits::ZERO
    };
    let root = GrantNode::root(Limits {
        streams: 2,
        grants: 1,
        retained_items: 3,
        ..Limits::ZERO
    });
    let node = root
        .restrict(
            &[
                permission(Rights::PUBLISH).with_limits(allowance),
                Permission {
                    event: OTHER,
                    rights: Rights::PUBLISH,
                    limits: allowance,
                },
            ],
            Limits {
                streams: 2,
                retained_items: 3,
                ..Limits::ZERO
            },
        )
        .unwrap();
    let first = Arc::new(Notify::new());
    let second = Arc::new(Notify::new());
    node.register_stream(EVENT, &first).unwrap();
    node.register_stream(EVENT, &first).unwrap();
    node.register_stream(OTHER, &second).unwrap();
    assert_eq!(
        node.register_stream(EVENT, &Arc::new(Notify::new())),
        Err(TopicError::Capacity)
    );
    let lease = node.try_charge_stream(EVENT, allowance).unwrap();
    let first_wake = Arc::new(WakeCount::default());
    let second_wake = Arc::new(WakeCount::default());
    let first_waker = first_wake.clone().into();
    let second_waker = second_wake.clone().into();
    {
        let mut first_changed = core::pin::pin!(first.notified());
        let mut second_changed = core::pin::pin!(second.notified());
        assert!(first_changed
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending());
        assert!(second_changed
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending());
        assert!(matches!(
            node.try_charge_stream(EVENT, allowance),
            Err(TopicError::Capacity)
        ));
        drop(
            node.try_charge(Limits {
                retained_items: 1,
                ..Limits::ZERO
            })
            .unwrap(),
        );
        assert_eq!(first_wake.0.load(Ordering::Relaxed), 0);
        assert_eq!(second_wake.0.load(Ordering::Relaxed), 0);
        drop(lease);
        assert!(first_wake.0.load(Ordering::Relaxed) > 0);
        assert_eq!(second_wake.0.load(Ordering::Relaxed), 0);
        assert!(first_changed
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_ready());
        assert!(second_changed
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending());
    }
    drop(first);
    node.register_stream(EVENT, &Arc::new(Notify::new()))
        .unwrap();
}

#[test]
fn revocation_listeners_ignore_accounting_and_wake_for_the_subtree_cutoff() {
    let root = GrantNode::root(Limits {
        grants: 1,
        retained_items: 1,
        ..Limits::ZERO
    });
    let child = root.restrict(&[], Limits::ZERO).unwrap();
    let wake = Arc::new(WakeCount::default());
    let waker = wake.clone().into();
    let mut context = Context::from_waker(&waker);
    let mut root_revoked = core::pin::pin!(root.revocation_changed.notified());
    let mut child_revoked = core::pin::pin!(child.revocation_changed.notified());
    assert!(root_revoked.as_mut().poll(&mut context).is_pending());
    assert!(child_revoked.as_mut().poll(&mut context).is_pending());

    drop(root.enter(EVENT, Rights::PUBLISH).unwrap());
    drop(
        root.try_charge(Limits {
            retained_items: 1,
            ..Limits::ZERO
        })
        .unwrap(),
    );
    assert_eq!(wake.0.load(Ordering::Relaxed), 0);
    assert!(root_revoked.as_mut().poll(&mut context).is_pending());
    assert!(child_revoked.as_mut().poll(&mut context).is_pending());

    let mut revoke = core::pin::pin!(root.revoke());
    assert_eq!(
        revoke.as_mut().poll(&mut context),
        core::task::Poll::Ready(Ok(()))
    );
    assert!(wake.0.load(Ordering::Relaxed) > 0);
    assert!(root_revoked.as_mut().poll(&mut context).is_ready());
    assert!(child_revoked.as_mut().poll(&mut context).is_ready());
}
