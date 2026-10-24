use core::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};
use rama_core::extensions::Extension;

use crate::address::ip::IntoCanonicalIpAddr as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash, Extension)]
#[extension(tags(net))]
/// Enum representing the IP modes that can be used by the DNS resolver.
pub enum DnsResolveIpMode {
    #[default]
    Dual,
    SingleIpV4,
    SingleIpV6,
    DualPreferIpV4,
}

impl DnsResolveIpMode {
    /// checks if IPv4 is supported in current mode
    #[must_use]
    pub fn ipv4_supported(&self) -> bool {
        matches!(self, Self::Dual | Self::SingleIpV4 | Self::DualPreferIpV4)
    }

    /// checks if IPv6 is supported in current mode
    #[must_use]
    pub fn ipv6_supported(&self) -> bool {
        matches!(self, Self::Dual | Self::SingleIpV6 | Self::DualPreferIpV4)
    }
}

/// Mode for establishing a connection.
///
/// Classification is by wire family: an IPv4-mapped IPv6 address
/// (`::ffff:a.b.c.d`, [RFC 4291, Section 2.5.5.2]) counts as IPv4,
/// so [`Self::Ipv6`] rejects it and [`Self::Ipv4`] accepts it.
///
/// [RFC 4291, Section 2.5.5.2]: https://datatracker.ietf.org/doc/html/rfc4291#section-2.5.5.2
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash, Extension)]
#[extension(tags(net))]
pub enum ConnectIpMode {
    #[default]
    Dual,
    Ipv4,
    Ipv6,
}

/// A destination rejected by the selected wire-address-family policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamilyRefusal {
    Ipv4 {
        requested: IpAddr,
        canonical: Ipv4Addr,
    },
    Ipv6 {
        requested: IpAddr,
        canonical: Ipv6Addr,
    },
}

impl fmt::Display for AddressFamilyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ipv4 {
                requested,
                canonical,
            } => write!(
                f,
                "IPv4 address {canonical} (requested as {requested}) is not allowed"
            ),
            Self::Ipv6 {
                requested,
                canonical,
            } => write!(
                f,
                "IPv6 address {canonical} (requested as {requested}) is not allowed"
            ),
        }
    }
}

impl core::error::Error for AddressFamilyRefusal {}

impl ConnectIpMode {
    /// Validate the destination's wire family and return its canonical address.
    ///
    /// Shared by IP-literal connectors and DNS address selection. Mapped IPv6
    /// addresses become IPv4 before applying the connection policy.
    pub fn validate_ip(self, requested: IpAddr) -> Result<IpAddr, AddressFamilyRefusal> {
        let ip = requested.into_canonical_ip_addr();
        match (ip, self) {
            (IpAddr::V4(canonical), Self::Ipv6) => Err(AddressFamilyRefusal::Ipv4 {
                requested,
                canonical,
            }),
            (IpAddr::V6(canonical), Self::Ipv4) => Err(AddressFamilyRefusal::Ipv6 {
                requested,
                canonical,
            }),
            _ => Ok(ip),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AddressFamilyRefusal, ConnectIpMode};
    use core::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn connect_ip_modes_classify_canonical_wire_families()
    -> Result<(), (ConnectIpMode, IpAddr, Result<IpAddr, AddressFamilyRefusal>)> {
        let ipv4 = Ipv4Addr::LOCALHOST;
        let ipv6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        for address in [IpAddr::V4(ipv4), IpAddr::V6(ipv4.to_ipv6_mapped())] {
            for mode in [ConnectIpMode::Dual, ConnectIpMode::Ipv4] {
                let received = mode.validate_ip(address);
                if received != Ok(IpAddr::V4(ipv4)) {
                    return Err((mode, address, received));
                }
            }
            let received = ConnectIpMode::Ipv6.validate_ip(address);
            if received
                != Err(AddressFamilyRefusal::Ipv4 {
                    requested: address,
                    canonical: ipv4,
                })
            {
                return Err((ConnectIpMode::Ipv6, address, received));
            }
        }
        for mode in [ConnectIpMode::Dual, ConnectIpMode::Ipv6] {
            let received = mode.validate_ip(ipv6);
            if received != Ok(ipv6) {
                return Err((mode, ipv6, received));
            }
        }
        let received = ConnectIpMode::Ipv4.validate_ip(ipv6);
        if received
            != Err(AddressFamilyRefusal::Ipv6 {
                requested: ipv6,
                canonical: Ipv6Addr::LOCALHOST,
            })
        {
            return Err((ConnectIpMode::Ipv4, ipv6, received));
        }
        Ok(())
    }
}
