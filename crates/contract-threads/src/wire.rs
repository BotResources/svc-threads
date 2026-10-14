//! Wire literals of the published enums.
//!
//! Every enum that crosses the frontier is mapped to its literal by hand, one
//! pair per variant, in both directions: a renamed variant never changes the
//! wire, and an unknown literal is a typed error, never a default variant.

/// A literal that names no variant of a published enum.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{literal}` is not a {kind} literal")]
pub struct UnknownLiteral {
    pub kind: &'static str,
    pub literal: String,
}

/// Declares a published enum and its total mapping to and from its wire
/// literals (`as_str` / `parse`), its `Display` and its serde form (the
/// literal as a JSON string).
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident => $literal:literal ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: &'static [Self] = &[ $( Self::$variant ),+ ];

            /// The wire literal.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $literal ),+
                }
            }

            /// The variant a wire literal names.
            pub fn parse(literal: &str) -> Result<Self, $crate::wire::UnknownLiteral> {
                match literal {
                    $( $literal => Ok(Self::$variant), )+
                    other => Err($crate::wire::UnknownLiteral {
                        kind: stringify!($name),
                        literal: other.to_owned(),
                    }),
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let literal = <String as ::serde::Deserialize>::deserialize(deserializer)?;
                Self::parse(&literal).map_err(::serde::de::Error::custom)
            }
        }
    };
}

pub(crate) use wire_enum;
