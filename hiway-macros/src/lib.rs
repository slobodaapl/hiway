use proc_macro::TokenStream;
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{quote, ToTokens};
use std::collections::BTreeSet;
use syn::{parse_quote, Data, DeriveInput, Error, Fields, Result};

#[proc_macro_derive(HiwayEvent)]
pub fn derive_hiway_event(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn expand(input: &DeriveInput) -> Result<TokenStream2> {
    let Data::Enum(enum_data) = &input.data else {
        return Err(Error::new_spanned(
            input,
            "HiwayEvent can only be derived for enums",
        ));
    };

    let mut payloads = Vec::with_capacity(enum_data.variants.len());
    let mut seen = BTreeSet::new();
    for variant in &enum_data.variants {
        let payload = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => &fields.unnamed[0].ty,
            Fields::Unnamed(_) | Fields::Unit | Fields::Named(_) => {
                return Err(Error::new_spanned(
                    &variant.fields,
                    "HiwayEvent variants must have exactly one unnamed field",
                ));
            }
        };

        let syntax = payload.to_token_stream().to_string();
        if !seen.insert(syntax) {
            return Err(Error::new_spanned(
                payload,
                "HiwayEvent payload types must be syntactically unique",
            ));
        }
        payloads.push(payload);
    }

    let hiway = hiway_path()?;
    let enum_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let mut marker_generics = input.generics.clone();
    marker_generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(
            #enum_name #ty_generics: ::core::clone::Clone
                + ::core::marker::Send
                + ::core::marker::Sync
                + 'static
        ));
    let (marker_impl_generics, marker_ty_generics, marker_where_clause) =
        marker_generics.split_for_impl();
    let variants = enum_data.variants.iter().map(|variant| &variant.ident);

    let conversions = variants
        .zip(payloads)
        .map(|(variant, payload)| {
            quote! {
                impl #impl_generics ::core::convert::From<#payload>
                    for #enum_name #ty_generics #where_clause
                {
                    fn from(value: #payload) -> Self {
                        Self::#variant(value)
                    }
                }

                impl #impl_generics ::core::convert::TryFrom<#enum_name #ty_generics>
                    for #payload #where_clause
                {
                    type Error = #enum_name #ty_generics;

                    fn try_from(value: #enum_name #ty_generics) -> ::core::result::Result<Self, Self::Error> {
                        match value {
                            #enum_name::#variant(payload) => ::core::result::Result::Ok(payload),
                            other => ::core::result::Result::Err(other),
                        }
                    }
                }

                impl #impl_generics #hiway::__private::TransformInput<#enum_name #ty_generics>
                    for #payload #where_clause
                {
                    fn try_from_event(
                        event: #enum_name #ty_generics,
                    ) -> ::core::result::Result<Self, #enum_name #ty_generics> {
                        <Self as ::core::convert::TryFrom<#enum_name #ty_generics>>::try_from(event)
                    }
                }
            }
        });

    Ok(quote! {
        impl #marker_impl_generics #hiway::HiwayEvent for #enum_name #marker_ty_generics #marker_where_clause {}

        impl #impl_generics #hiway::__private::TransformInput<#enum_name #ty_generics>
            for #enum_name #ty_generics #where_clause
        {
            fn try_from_event(
                event: #enum_name #ty_generics,
            ) -> ::core::result::Result<Self, #enum_name #ty_generics> {
                ::core::result::Result::Ok(event)
            }
        }

        #(#conversions)*
    })
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
            format!("HiwayEvent could not locate the hiway dependency: {error}"),
        )),
    }
}
