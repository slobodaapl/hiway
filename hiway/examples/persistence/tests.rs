use super::{run_once, WorkerState};

#[tokio::test]
async fn json_snapshot_reloads_for_a_fresh_worker_run() {
    let first = run_once(WorkerState::default(), 17).await.unwrap();
    assert_eq!(first.completed, 1);
    assert_eq!(first.last_job, Some(17));

    let snapshot = serde_json::to_string(&first).unwrap();
    let json: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
    let object = json.as_object().unwrap();
    assert_eq!(object.len(), 2);
    assert!(object.contains_key("completed"));
    assert!(object.contains_key("last_job"));

    let restored: WorkerState = serde_json::from_str(&snapshot).unwrap();
    assert_eq!(restored.completed, first.completed);
    assert_eq!(restored.last_job, first.last_job);

    let second = run_once(restored, 42).await.unwrap();
    assert_eq!(second.completed, 2);
    assert_eq!(second.last_job, Some(42));
}

#[tokio::test]
async fn worker_state_completion_overflow_returns_error() {
    let state = WorkerState {
        completed: u32::MAX,
        last_job: Some(17),
    };
    assert!(run_once(state, 42).await.is_err());
}
