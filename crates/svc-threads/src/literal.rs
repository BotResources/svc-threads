//! The literals Threads writes for its own small enums — a fact's reason, a
//! delta's cause — mapped by hand, one pair per variant: a renamed variant
//! never changes what is stored or sent.

/// Declares an enum, its literal (`as_str`) and its serde form (the literal as
/// a JSON string).
macro_rules! literal_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident => $literal:literal ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $literal ),+
                }
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }
    };
}

pub(crate) use literal_enum;

#[cfg(test)]
mod tests {
    literal_enum! {
        pub enum Sample {
            One => "ONE",
            TwoWords => "TWO_WORDS",
        }
    }

    #[test]
    fn a_literal_is_written_as_declared() {
        assert_eq!(Sample::TwoWords.as_str(), "TWO_WORDS");
        assert_eq!(
            serde_json::to_value(Sample::One).unwrap(),
            serde_json::json!("ONE")
        );
    }
}
