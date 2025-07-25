use core::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FragmentType {
    Full = 0,
    Delta = 1,
}

impl Display for FragmentType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FragmentType::Full => write!(f, "full"),
            FragmentType::Delta => write!(f, "delta"),
        }
    }
}

pub(crate) mod hltv_download;
pub(crate) mod hltv_extract_meta;
