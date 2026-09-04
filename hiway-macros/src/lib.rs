#![doc = "Attribute macros for hiway event specifications and injected ports."]
#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote, ToTokens};
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input, Error, Fields, Ident as SynIdent, ItemEnum, ItemStruct, LitInt, Path,
    Result, Token,
};

#[proc_macro_attribute]
pub fn events(attributes: TokenStream, item: TokenStream) -> TokenStream {
    let attributes = parse_macro_input!(attributes as EventsArgs);
    let item = parse_macro_input!(item as ItemEnum);
    match expand_events(&attributes, &item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn port(attributes: TokenStream, item: TokenStream) -> TokenStream {
    let attributes = parse_macro_input!(attributes as PortArgs);
    let item = parse_macro_input!(item as ItemStruct);
    match expand_port(&attributes, &item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn graph(attributes: TokenStream, item: TokenStream) -> TokenStream {
    let attributes = parse_macro_input!(attributes as GraphArgs);
    let item = parse_macro_input!(item as ItemStruct);
    match expand_graph(&attributes, &item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

struct EventsArgs {
    wire_major: Option<LitInt>,
    schema_revision: Option<LitInt>,
}

impl Parse for EventsArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut wire_major = None;
        let mut schema_revision = None;
        while !input.is_empty() {
            let key: SynIdent = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "wire_major" if wire_major.is_none() => wire_major = Some(input.parse()?),
                "schema_revision" if schema_revision.is_none() => {
                    schema_revision = Some(input.parse()?);
                }
                "wire_major" | "schema_revision" => {
                    return Err(Error::new(key.span(), "event version field repeated"));
                }
                _ => return Err(Error::new(key.span(), "unknown event declaration field")),
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self {
            wire_major,
            schema_revision,
        })
    }
}

fn expand_events(arguments: &EventsArgs, item: &ItemEnum) -> Result<TokenStream2> {
    if !item.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &item.generics,
            "event declarations with generics are not supported",
        ));
    }

    let hiway = hiway_path()?;
    let visibility = &item.vis;
    let enum_name = &item.ident;
    let module = format_ident!("{}", snake_case(&enum_name.to_string()));
    let wire_constants = match (&arguments.wire_major, &arguments.schema_revision) {
        (None, None) => quote!(),
        (wire_major, schema_revision) => {
            let wire_major = wire_major.as_ref().map(
                |literal| quote!(const WIRE_MAJOR: #hiway::WireMajor = #hiway::WireMajor(#literal);),
            );
            let schema_revision = schema_revision.as_ref().map(|literal| {
                quote!(const SCHEMA_REVISION: #hiway::SchemaRevision = #hiway::SchemaRevision(#literal);)
            });
            quote!(#wire_major #schema_revision)
        }
    };
    let marker_visibility = if matches!(visibility, syn::Visibility::Inherited) {
        quote!(pub(crate))
    } else {
        quote!(pub)
    };

    let mut markers = Vec::with_capacity(item.variants.len());
    let mut implementations = Vec::with_capacity(item.variants.len());
    let mut enum_conversions = Vec::with_capacity(item.variants.len());
    for variant in &item.variants {
        let name = &variant.ident;
        let (payload, enum_value, conversion_arm, constructor) = match &variant.fields {
            Fields::Unit => (
                quote!(()),
                quote!({ let () = value.into_inner(); Self::#name }),
                quote!(#enum_name::#name => ::core::result::Result::Ok(#hiway::EventValue::new(()))),
                quote! {
                    #[allow(non_upper_case_globals)]
                    #marker_visibility const #name: #hiway::EventValue<#name> =
                        #hiway::EventValue::new(());
                },
            ),
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let payload = &fields.unnamed[0].ty;
                (
                    quote!(#payload),
                    quote!(Self::#name(value.into_inner())),
                    quote!(#enum_name::#name(payload) => ::core::result::Result::Ok(#hiway::EventValue::new(payload))),
                    quote! {
                        #[allow(non_snake_case)]
                        #marker_visibility const fn #name(
                            payload: <#name as #hiway::EventSpec>::Payload,
                        ) -> #hiway::EventValue<#name> {
                            #hiway::EventValue::new(payload)
                        }
                    },
                )
            }
            Fields::Unnamed(_) | Fields::Named(_) => {
                return Err(Error::new_spanned(
                    &variant.fields,
                    "event variants must be unit-like or carry one unnamed payload",
                ));
            }
        };

        markers.push(quote! {
            #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
            #marker_visibility struct #name {
                _private: (),
            }

            #constructor
        });
        let event_id = quote! {
            #hiway::EventId::from_name(concat!(
                module_path!(),
                "::",
                stringify!(#enum_name),
                "::",
                stringify!(#name),
            ))
        };
        implementations.push(quote! {
            impl #hiway::EventSpec for #module::#name {
                type Payload = #payload;
                const ID: #hiway::EventId = #event_id;
                #wire_constants
            }
        });
        enum_conversions.push(quote! {
            impl ::core::convert::From<#hiway::EventValue<#module::#name>> for #enum_name {
                fn from(value: #hiway::EventValue<#module::#name>) -> Self {
                    #enum_value
                }
            }

            impl ::core::convert::TryFrom<#enum_name> for #hiway::EventValue<#module::#name> {
                type Error = #enum_name;

                fn try_from(value: #enum_name) -> ::core::result::Result<Self, Self::Error> {
                    match value {
                        #conversion_arm,
                        other => ::core::result::Result::Err(other),
                    }
                }
            }
        });
    }

    Ok(quote! {
        #[allow(dead_code)]
        #item

        #visibility mod #module {
            #(#markers)*
        }

        #(#implementations)*
        #(#enum_conversions)*
    })
}

struct PortArgs {
    factory: Path,
    send: Vec<Path>,
    recv: Vec<Path>,
    capacity: Option<LitInt>,
}

impl Parse for PortArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut factory = None;
        let mut send = None;
        let mut recv = None;
        let mut capacity = None;
        while !input.is_empty() {
            let key: SynIdent = input.parse()?;
            match key.to_string().as_str() {
                "factory" if factory.is_none() => {
                    input.parse::<Token![=]>()?;
                    factory = Some(input.parse()?);
                }
                "factory" => return Err(Error::new(key.span(), "port factory repeated")),
                "send" | "recv" => {
                    let content;
                    parenthesized!(content in input);
                    let paths = content
                        .parse_terminated(Path::parse, Token![,])?
                        .into_iter()
                        .collect::<Vec<_>>();
                    match key.to_string().as_str() {
                        "send" if send.is_none() => send = Some(paths),
                        "recv" if recv.is_none() => recv = Some(paths),
                        _ => {
                            return Err(Error::new(key.span(), "port capability group repeated"));
                        }
                    }
                }
                "capacity" if capacity.is_none() => {
                    input.parse::<Token![=]>()?;
                    capacity = Some(input.parse()?);
                }
                "capacity" => {
                    return Err(Error::new(key.span(), "port capacity repeated"));
                }
                _ => {
                    return Err(Error::new(
                        key.span(),
                        "expected `factory = Type`, `send(...)`, `recv(...)`, or `capacity = N`",
                    ));
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self {
            factory: factory.ok_or_else(|| input.error("port requires `factory = Type`"))?,
            send: send.unwrap_or_default(),
            recv: recv.unwrap_or_default(),
            capacity,
        })
    }
}

struct GraphArgs {
    entries: Vec<GraphEntry>,
}

struct GraphEntry {
    field: SynIdent,
    event: Path,
    capacity: LitInt,
}

impl Parse for GraphArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut entries = Vec::new();
        while !input.is_empty() {
            let field: SynIdent = input.parse()?;
            input.parse::<Token![=]>()?;
            let content;
            parenthesized!(content in input);
            let event: Path = content.parse()?;
            content.parse::<Token![,]>()?;
            let capacity: LitInt = content.parse()?;
            if !content.is_empty() {
                return Err(content.error("expected `(event, capacity)`"));
            }
            entries.push(GraphEntry {
                field,
                event,
                capacity,
            });
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        if entries.is_empty() {
            return Err(input.error("graph must declare at least one event"));
        }
        Ok(Self { entries })
    }
}

fn expand_graph(arguments: &GraphArgs, item: &ItemStruct) -> Result<TokenStream2> {
    if !item.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &item.generics,
            "graph declarations with generics are not supported",
        ));
    }
    if !matches!(item.fields, Fields::Unit) {
        return Err(Error::new_spanned(
            &item.fields,
            "graph declarations must be unit structs",
        ));
    }

    let mut fields = std::collections::BTreeSet::new();
    let mut events = std::collections::BTreeSet::new();
    for entry in &arguments.entries {
        if !fields.insert(entry.field.to_string()) {
            return Err(Error::new_spanned(&entry.field, "graph field repeated"));
        }
        let event_name = entry.event.to_token_stream().to_string();
        if !events.insert(event_name) {
            return Err(Error::new_spanned(&entry.event, "graph event repeated"));
        }
    }

    let hiway = hiway_path()?;
    let graph = &item.ident;
    let visibility = &item.vis;
    let attrs = &item.attrs;
    let fields_definitions = arguments.entries.iter().map(|entry| {
        let field = &entry.field;
        let event = &entry.event;
        let capacity = &entry.capacity;
        quote! {
            #field: #hiway::StaticFabric<'route, 'target, #event, #capacity>
        }
    });
    let constructor_parameters = arguments.entries.iter().map(|entry| {
        let field = &entry.field;
        let event = &entry.event;
        let capacity = &entry.capacity;
        quote! {
            #field: &'route #hiway::HeaplessRoute<'target, #event, #capacity>
        }
    });
    let constructor_fields = arguments.entries.iter().map(|entry| {
        let field = &entry.field;
        quote! { #field: #hiway::StaticFabric::new(#field) }
    });
    let binding_impls = arguments.entries.iter().map(|entry| {
        let field = &entry.field;
        let event = &entry.event;
        let capacity = &entry.capacity;
        quote! {
            impl<'route, 'target> #hiway::PortBinding<'target, #event>
                for #graph<'route, 'target>
            where
                <#event as #hiway::EventSpec>::Payload: Clone,
            {
                type Sender = <#hiway::StaticFabric<
                    'route,
                    'target,
                    #event,
                    #capacity,
                > as #hiway::PortBinding<'target, #event>>::Sender;
                type Subscription = <#hiway::StaticFabric<
                    'route,
                    'target,
                    #event,
                    #capacity,
                > as #hiway::PortBinding<'target, #event>>::Subscription;

                fn sender(&self) -> ::core::result::Result<Self::Sender, #hiway::TopicError> {
                    <#hiway::StaticFabric<
                        'route,
                        'target,
                        #event,
                        #capacity,
                    > as #hiway::PortBinding<'target, #event>>::sender(&self.#field)
                }

                fn subscribe_mapped<T, H>(
                    &self,
                    target: &'target #hiway::MappedTarget<
                        <#event as #hiway::EventSpec>::Payload,
                        T,
                        H,
                    >,
                    policy: #hiway::DeliveryPolicy,
                ) -> ::core::result::Result<Self::Subscription, #hiway::TopicError>
                where
                    T: 'target,
                    H: #hiway::Target<T> + Clone + Sync + 'target,
                {
                    <#hiway::StaticFabric<
                        'route,
                        'target,
                        #event,
                        #capacity,
                    > as #hiway::PortBinding<'target, #event>>::subscribe_mapped(
                        &self.#field,
                        target,
                        policy,
                    )
                }
            }
        }
    });

    Ok(quote! {
        #(#attrs)*
        #visibility struct #graph<'route, 'target> {
            #(#fields_definitions,)*
        }

        impl<'route, 'target> #graph<'route, 'target> {
            #[must_use]
            pub const fn new(
                #(#constructor_parameters,)*
            ) -> Self {
                Self {
                    #(#constructor_fields,)*
                }
            }

            pub fn sender<S>(&self) -> ::core::result::Result<
                <Self as #hiway::PortBinding<'target, S>>::Sender,
                #hiway::TopicError,
            >
            where
                S: #hiway::EventSpec,
                Self: #hiway::PortBinding<'target, S>,
            {
                <Self as #hiway::PortBinding<'target, S>>::sender(self)
            }
        }

        #(#binding_impls)*
    })
}

#[allow(clippy::too_many_lines)]
fn expand_port(arguments: &PortArgs, item: &ItemStruct) -> Result<TokenStream2> {
    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return Err(Error::new_spanned(
            &item.generics,
            "port declarations with generics or where clauses are not supported",
        ));
    }
    if !matches!(item.fields, Fields::Unit) {
        return Err(Error::new_spanned(
            &item.fields,
            "ports must be unit structs; application state belongs in the owning type",
        ));
    }

    let hiway = hiway_path()?;
    let factory = &arguments.factory;
    let port = &item.ident;
    let visibility = &item.vis;
    let attrs = &item.attrs;
    let capacity = arguments.capacity.as_ref().map_or_else(
        || quote!(#hiway::DEFAULT_INBOX_CAPACITY),
        |literal| quote!(#literal),
    );
    let send = capability_list(&arguments.send, "send")?;
    let recv = capability_list(&arguments.recv, "recv")?;

    let sender_fields = send.iter().map(|(event, _, field)| {
        let field = format_ident!("__send_{field}");
        quote!(#field: <#factory as #hiway::OwnedPortBinding<#event>>::Sender)
    });
    let sender_initializers = send.iter().map(|(event, _, field)| {
        let field = format_ident!("__send_{field}");
        quote!(
            #field: <#factory as #hiway::OwnedPortBinding<#event>>::sender_owned(factory)?
        )
    });
    let receiver_fields = recv.iter().map(|(event, _, field)| {
        let field = format_ident!("__recv_{field}");
        quote!(#field: #hiway::OwnedEndpoint<#factory, #event, { #capacity }>)
    });
    let receiver_initializers = recv.iter().map(|(_, _, field)| {
        let field = format_ident!("__recv_{field}");
        quote!(#field: #hiway::OwnedEndpoint::bind(factory)?)
    });

    let port_impl = quote!(impl #hiway::Port for #port {});
    let publication_impls = send.iter().map(|(event, _, field)| {
        let sender_field = format_ident!("__send_{field}");
        quote! {
            impl #hiway::EventPort<#event> for #port {
                type Sender = <#factory as #hiway::OwnedPortBinding<#event>>::Sender;

                fn event_sender(&self) -> &Self::Sender {
                    &self.#sender_field
                }
            }
        }
    });
    let receiver_impls = recv.iter().map(|(event, _, field)| {
        let receiver_field = format_ident!("__recv_{field}");
        quote! {
            impl #hiway::EventReceiver<#event> for #port {
                fn event_try_recv(&self) -> ::core::option::Option<
                    <#event as #hiway::EventSpec>::Payload,
                > {
                    self.#receiver_field.try_recv()
                }

                fn event_recv(
                    &self,
                ) -> impl ::core::future::Future<
                    Output = ::core::option::Option<
                        <#event as #hiway::EventSpec>::Payload,
                    >,
                > + '_ {
                    self.#receiver_field.recv()
                }
            }
        }
    });

    let named_publish_methods = send.iter().map(|(event, _, field)| {
        let publish = format_ident!("publish_{field}");
        let try_publish = format_ident!("try_publish_{field}");
        quote! {
            pub fn #publish(
                &self,
                payload: <#event as #hiway::EventSpec>::Payload,
            ) -> #hiway::SendFuture<
                '_,
                #event,
                <Self as #hiway::EventPort<#event>>::Sender,
            > {
                <Self as #hiway::PortExt>::publish::<#event>(
                    self,
                    #hiway::EventValue::new(payload),
                )
            }

            pub fn #try_publish(
                &self,
                payload: <#event as #hiway::EventSpec>::Payload,
            ) -> ::core::result::Result<
                (),
                #hiway::TrySendError<<#event as #hiway::EventSpec>::Payload>,
            > {
                <Self as #hiway::PortExt>::try_publish::<#event>(
                    self,
                    #hiway::EventValue::new(payload),
                )
            }
        }
    });
    let named_receive_methods = recv.iter().map(|(event, _, field)| {
        let recv = format_ident!("recv_{field}");
        let try_recv = format_ident!("try_recv_{field}");
        quote! {
            pub fn #recv(
                &self,
            ) -> impl ::core::future::Future<
                Output = ::core::option::Option<
                    <#event as #hiway::EventSpec>::Payload,
                >,
            > + '_ {
                <Self as #hiway::EventReceiver<#event>>::event_recv(self)
            }

            pub fn #try_recv(
                &self,
            ) -> ::core::option::Option<<#event as #hiway::EventSpec>::Payload> {
                <Self as #hiway::EventReceiver<#event>>::event_try_recv(self)
            }
        }
    });

    Ok(quote! {
        #(#attrs)*
        #visibility struct #port {
            #(#sender_fields,)*
            #(#receiver_fields,)*
        }

        impl #port {
            pub fn bind(
                factory: &#factory,
            ) -> ::core::result::Result<Self, #hiway::PortError> {
                Ok(Self {
                    #(#sender_initializers,)*
                    #(#receiver_initializers,)*
                })
            }

            #(#named_publish_methods)*
            #(#named_receive_methods)*
        }

        #port_impl
        #(#publication_impls)*
        #(#receiver_impls)*
    })
}

fn capability_list(paths: &[Path], label: &str) -> Result<Vec<(Path, Ident, Ident)>> {
    let mut names = std::collections::BTreeSet::new();
    let mut capabilities = Vec::with_capacity(paths.len());
    for path in paths {
        let Some(segment) = path.segments.last() else {
            return Err(Error::new_spanned(
                path,
                "event specification path is empty",
            ));
        };
        let variant = segment.ident.clone();
        if !names.insert(variant.to_string()) {
            return Err(Error::new_spanned(
                path,
                format!("duplicate {label} event `{variant}`"),
            ));
        }
        let field = format_ident!("{}", snake_case(&variant.to_string()));
        capabilities.push((path.clone(), variant, field));
    }
    Ok(capabilities)
}

fn snake_case(name: &str) -> String {
    let mut output = String::with_capacity(name.len() + 4);
    let characters = name.chars().collect::<Vec<_>>();
    for (index, character) in characters.iter().copied().enumerate() {
        let previous_is_lowercase = index
            .checked_sub(1)
            .and_then(|previous| characters.get(previous))
            .is_some_and(|previous| previous.is_lowercase() || previous.is_ascii_digit());
        let next_is_lowercase = characters
            .get(index + 1)
            .is_some_and(|next| next.is_lowercase());
        if character.is_uppercase() && index != 0 && (previous_is_lowercase || next_is_lowercase) {
            output.push('_');
        }
        output.extend(character.to_lowercase());
    }
    output
}

fn hiway_path() -> Result<TokenStream2> {
    match crate_name("hiway") {
        Ok(FoundCrate::Itself) => Ok(quote!(::hiway)),
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            Ok(quote!(::#ident))
        }
        Err(error) => Err(Error::new(
            Span::call_site(),
            format!("hiway macro could not locate the hiway dependency: {error}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_and_port_arguments_reject_removed_or_missing_contracts() {
        let namespace = syn::parse_str::<EventsArgs>(r#"namespace = "legacy""#)
            .err()
            .expect("namespace must be rejected");
        assert!(namespace.to_string().contains("unknown event declaration"));

        let missing_factory = syn::parse_str::<PortArgs>("send(events::Started)")
            .err()
            .expect("factory must be required");
        assert!(missing_factory.to_string().contains("factory"));

        let removed_flag = syn::parse_str::<PortArgs>("factory = Bus, no_std")
            .err()
            .expect("removed flag must be rejected");
        assert!(removed_flag.to_string().contains("expected"));
    }

    #[test]
    fn port_rejects_generic_and_stateful_declarations() {
        let arguments = syn::parse_str::<PortArgs>("factory = Bus").unwrap();
        let generic = syn::parse_str::<ItemStruct>("struct PlayerPort<T>;").unwrap();
        assert!(expand_port(&arguments, &generic)
            .unwrap_err()
            .to_string()
            .contains("generics"));

        let stateful = syn::parse_str::<ItemStruct>("struct Player { health: u8 }").unwrap();
        assert!(expand_port(&arguments, &stateful)
            .unwrap_err()
            .to_string()
            .contains("application state"));
    }

    #[test]
    fn marker_modules_use_rust_snake_case() {
        assert_eq!(snake_case("GameEvents"), "game_events");
        assert_eq!(snake_case("HTTPEvents"), "http_events");
    }
}
