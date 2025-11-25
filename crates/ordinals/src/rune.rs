use super::*;

#[derive(
  Default, Debug, PartialEq, Copy, Clone, PartialOrd, Ord, Eq, Hash, DeserializeFromStr, SerializeDisplay,
)]
pub struct Rune(pub u128);

impl Rune {
  const RESERVED: u128 = 6402364363415443603228541259936211926;
  pub const FRACTAL_START_INTERVAL: u32 = 21000;
  pub const FRACTAL_SUBSIDY_HALVING_INTERVAL: u32 = 2_100_000 ;
  const INTERVAL: u32 = Self::FRACTAL_SUBSIDY_HALVING_INTERVAL / 12;

  const STEPS: &'static [u128] = &[
    0,
    26,
    702,
    18278,
    475254,
    12356630,
    321272406,
    8353082582,
    217180147158,
    5646683826134,
    146813779479510,
    3817158266467286,
    99246114928149462,
    2580398988131886038,
    67090373691429037014,
    1744349715977154962390,
    45353092615406029022166,
    1179180408000556754576342,
    30658690608014475618984918,
    797125955808376366093607894,
    20725274851017785518433805270,
    538857146126462423479278937046,
    14010285799288023010461252363222,
    364267430781488598271992561443798,
    9470953200318703555071806597538774,
    246244783208286292431866971536008150,
    6402364363415443603228541259936211926,
    166461473448801533683942072758341510102,
  ];

  pub fn n(self) -> u128 {
    self.0
  }

  pub fn first_rune_height(network: Network) -> u32 {
    Self::FRACTAL_START_INTERVAL
      * match network {
        Network::Bitcoin => 4,
        Network::Regtest => 0,
        Network::Signet => 0,
        Network::Testnet => 12,
        
        _ => 0,
      }
  }

  pub fn minimum_at_height(chain: Network, height: Height) -> Self {
    let offset = height.0.saturating_add(1);

    let start = Self::first_rune_height(chain);

    let end = start + Self::FRACTAL_SUBSIDY_HALVING_INTERVAL;

    if offset < start {
      return Rune(Self::STEPS[12]);
    }

    if offset >= end {
      return Rune(0);
    }

    let progress = offset.saturating_sub(start);

    let length = 12u32.saturating_sub(progress / Self::INTERVAL);

    let end = Self::STEPS[usize::try_from(length - 1).unwrap()];

    let start = Self::STEPS[usize::try_from(length).unwrap()];

    let remainder = u128::from(progress % Self::INTERVAL);

    Rune(start - ((start - end) * remainder / u128::from(Self::INTERVAL)))
  }

  pub fn is_reserved(self) -> bool {
    self.0 >= Self::RESERVED
  }

  pub fn reserved(block: u64, tx: u32) -> Self {
    Self(
      Self::RESERVED
        .checked_add(u128::from(block) << 32 | u128::from(tx))
        .unwrap(),
    )
  }

  pub fn commitment(self) -> Vec<u8> {
    let bytes = self.0.to_le_bytes();

    let mut end = bytes.len();

    while end > 0 && bytes[end - 1] == 0 {
      end -= 1;
    }

    bytes[..end].into()
  }
}

impl Display for Rune {
  fn fmt(&self, f: &mut Formatter) -> fmt::Result {
    let mut n = self.0;
    if n == u128::MAX {
      return write!(f, "bcgdenlqrqwdslrugsnlbtmfijav");
    }

    n += 1;
    let mut symbol = String::new();
    while n > 0 {
      symbol.push(
        "abcdefghijklmnopqrstuvwxyz"
          .chars()
          .nth(((n - 1) % 26) as usize)
          .unwrap(),
      );
      n = (n - 1) / 26;
    }

    for c in symbol.chars().rev() {
      write!(f, "{c}")?;
    }

    Ok(())
  }
}

impl FromStr for Rune {
  type Err = Error;

  fn from_str(s: &str) -> Result<Self, Error> {
    let mut x = 0u128;
    for (i, c) in s.chars().enumerate() {
      if i > 0 {
        x = x.checked_add(1).ok_or(Error::Range)?;
      }
      x = x.checked_mul(26).ok_or(Error::Range)?;
      match c {
        'a'..='z' => {
          x = x.checked_add(c as u128 - 'a' as u128).ok_or(Error::Range)?;
        }
        _ => return Err(Error::Character(c)),
      }
    }
    Ok(Rune(x))
  }
}

#[derive(Debug, PartialEq)]
pub enum Error {
  Character(char),
  Range,
}

impl Display for Error {
  fn fmt(&self, f: &mut Formatter) -> fmt::Result {
    match self {
      Self::Character(c) => write!(f, "invalid character `{c}`"),
      Self::Range => write!(f, "name out of range"),
    }
  }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trip() {
    fn case(n: u128, s: &str) {
      assert_eq!(Rune(n).to_string(), s);
      assert_eq!(s.parse::<Rune>().unwrap(), Rune(n));
    }

    case(0, "a");
    case(1, "b");
    case(2, "c");
    case(3, "d");
    case(4, "e");
    case(5, "f");
    case(6, "g");
    case(7, "h");
    case(8, "i");
    case(9, "j");
    case(10, "k");
    case(11, "l");
    case(12, "m");
    case(13, "n");
    case(14, "o");
    case(15, "p");
    case(16, "q");
    case(17, "r");
    case(18, "s");
    case(19, "t");
    case(20, "u");
    case(21, "v");
    case(22, "w");
    case(23, "x");
    case(24, "y");
    case(25, "z");
    case(26, "aa");
    case(27, "ab");
    case(51, "az");
    case(52, "ba");
    case(u128::MAX - 2, "bcgdenlqrqwdslrugsnlbtmfijat");
    case(u128::MAX - 1, "bcgdenlqrqwdslrugsnlbtmfijau");
    case(u128::MAX, "bcgdenlqrqwdslrugsnlbtmfijav");
    case(2055900680524219742,"uncommongoods");
  }

  #[test]
  fn from_str_error() {
    assert_eq!(
      "bcgdenlqrqwdslrugsnlbtmfijaw".parse::<Rune>().unwrap_err(),
      Error::Range,
    );
    assert_eq!(
      "bcgdenlqrqwdslrugsnlbstmfijavx".parse::<Rune>().unwrap_err(),
      Error::Range,
    );
    assert_eq!("X".parse::<Rune>().unwrap_err(), Error::Character('X'));
  }

  #[test]
  #[allow(clippy::identity_op)]
  #[allow(clippy::erasing_op)]
  #[allow(clippy::zero_prefixed_literal)]
  fn mainnet_minimum_at_height() {
    #[track_caller]
    fn case(height: u32, minimum: &str) {
      assert_eq!(
        Rune::minimum_at_height(Network::Bitcoin, Height(height)).to_string(),
        minimum,
      );
    }

    const START: u32 = Rune::FRACTAL_START_INTERVAL * 4;
    const END: u32 = START + Rune::FRACTAL_SUBSIDY_HALVING_INTERVAL;
    const INTERVAL: u32 = Rune::FRACTAL_SUBSIDY_HALVING_INTERVAL / 12;

    case(0, "aaaaaaaaaaaaa");
    case(START / 2, "aaaaaaaaaaaaa");
    case(START, "zzzxkctymrzn");
    case(START + 1, "zzzuufnwzjza");
    case(END - 1, "a");
    case(END, "a");
    case(END + 1, "a");
    case(u32::MAX, "a");

    case(START + INTERVAL * 00 - 1, "aaaaaaaaaaaaa");
    case(START + INTERVAL * 00 + 0, "zzzxkctymrzn");
    case(START + INTERVAL * 00 + 1, "zzzuufnwzjza");

    case(START + INTERVAL * 01 - 1, "aaaaaaaaaaaa");
    case(START + INTERVAL * 01 + 0, "zzzxkctymsa");
    case(START + INTERVAL * 01 + 1, "zzzuufnwzjz");

    case(START + INTERVAL * 02 - 1, "aaaaaaaaaaa");
    case(START + INTERVAL * 02 + 0, "zzzxkctyms");
    case(START + INTERVAL * 02 + 1, "zzzuufnwzk");

    case(START + INTERVAL * 03 - 1, "aaaaaaaaaa");
    case(START + INTERVAL * 03 + 0, "zzzxkctyn");
    case(START + INTERVAL * 03 + 1, "zzzuufnxa");

    case(START + INTERVAL * 04 - 1, "aaaaaaaaa");
    case(START + INTERVAL * 04 + 0, "zzzxkctz");
    case(START + INTERVAL * 04 + 1, "zzzuufnx");

    case(START + INTERVAL * 05 - 1, "aaaaaaaa");
    case(START + INTERVAL * 05 + 0, "zzzxkcu");
    case(START + INTERVAL * 05 + 1, "zzzuufo");

    case(START + INTERVAL * 06 - 1, "aaaaaaa");
    case(START + INTERVAL * 06 + 0, "zzzxkd");
    case(START + INTERVAL * 06 + 1, "zzzuug");

    case(START + INTERVAL * 07 - 1, "aaaaaa");
    case(START + INTERVAL * 07 + 0, "zzzxl");
    case(START + INTERVAL * 07 + 1, "zzzuv");

    case(START + INTERVAL * 08 - 1, "aaaaa");
    case(START + INTERVAL * 08 + 0, "zzzy");
    case(START + INTERVAL * 08 + 1, "zzzv");

    case(START + INTERVAL * 09 - 1, "aaaa");
    case(START + INTERVAL * 09 + 0, "aaaa");
    case(START + INTERVAL * 09 + 1, "aaaa");

    case(START + INTERVAL * 10 - 2, "aab");
    case(START + INTERVAL * 10 - 1, "aaa");
    case(START + INTERVAL * 10 + 0, "aaa");
    case(START + INTERVAL * 10 + 1, "aaa");

    case(START + INTERVAL * 10 + INTERVAL / 2, "na");

    case(START + INTERVAL * 11 - 2, "ab");
    case(START + INTERVAL * 11 - 1, "aa");
    case(START + INTERVAL * 11 + 0, "aa");
    case(START + INTERVAL * 11 + 1, "aa");

    case(START + INTERVAL * 11 + INTERVAL / 2, "n");

    case(START + INTERVAL * 12 - 2, "b");
    case(START + INTERVAL * 12 - 1, "a");
    case(START + INTERVAL * 12 + 0, "a");
    case(START + INTERVAL * 12 + 1, "a");
  }

  #[test]
  fn minimum_at_height() {
    #[track_caller]
    fn case(network: Network, height: u32, minimum: &str) {
      assert_eq!(
        Rune::minimum_at_height(network, Height(height)).to_string(),
        minimum,
      );
    }

    case(Network::Testnet, 0, "aaaaaaaaaaaaa");
    case(
      Network::Testnet,
      Rune::FRACTAL_START_INTERVAL * 12 - 1,
      "aaaaaaaaaaaaa",
    );
    case(
      Network::Testnet,
      Rune::FRACTAL_START_INTERVAL * 12,
      "zzzxkctymrzn",
    );
    case(
      Network::Testnet,
      Rune::FRACTAL_START_INTERVAL * 12 + 1,
      "zzzuufnwzjza",
    );

    case(Network::Signet, 0, "zzzxkctymrzn");
    case(Network::Signet, 1, "zzzuufnwzjza");

    case(Network::Regtest, 0, "zzzxkctymrzn");
    case(Network::Regtest, 1, "zzzuufnwzjza");
  }

  #[test]
  fn serde() {
    let rune = Rune(0);
    let json = "\"a\"";
    assert_eq!(serde_json::to_string(&rune).unwrap(), json);
    assert_eq!(serde_json::from_str::<Rune>(json).unwrap(), rune);
  }

  #[test]
  fn reserved() {
    assert_eq!(
      Rune::RESERVED,
      "aaaaaaaaaaaaaaaaaaaaaaaaaaa".parse::<Rune>().unwrap().0,
    );

    assert_eq!(Rune::reserved(0, 0), Rune(Rune::RESERVED));
    assert_eq!(Rune::reserved(0, 1), Rune(Rune::RESERVED + 1));
    assert_eq!(Rune::reserved(1, 0), Rune(Rune::RESERVED + (1 << 32)));
    assert_eq!(Rune::reserved(1, 1), Rune(Rune::RESERVED + (1 << 32) + 1));
    assert_eq!(
      Rune::reserved(u64::MAX, u32::MAX),
      Rune(Rune::RESERVED + (u128::from(u64::MAX) << 32 | u128::from(u32::MAX))),
    );
  }

  #[test]
  fn is_reserved() {
    #[track_caller]
    fn case(rune: &str, reserved: bool) {
      assert_eq!(rune.parse::<Rune>().unwrap().is_reserved(), reserved);
    }

    case("a", false);
    case("zzzzzzzzzzzzzzzzzzzzzzzzzz", false);
    case("aaaaaaaaaaaaaaaaaaaaaaaaaaa", true);
    case("aaaaaaaaaaaaaaaaaaaaaaaaaab", true);
    case("bcgdenlqrqwdslrugsnlbtmfijav", true);
  }

  #[test]
  fn steps() {
    for i in 0.. {
      match "a".repeat(i + 1).parse::<Rune>() {
        Ok(rune) => assert_eq!(Rune(Rune::STEPS[i]), rune),
        Err(_) => {
          assert_eq!(Rune::STEPS.len(), i);
          break;
        }
      }
    }
  }

  #[test]
  fn commitment() {
    #[track_caller]
    fn case(rune: u128, bytes: &[u8]) {
      assert_eq!(Rune(rune).commitment(), bytes);
    }

    case(0, &[]);
    case(1, &[1]);
    case(255, &[255]);
    case(256, &[0, 1]);
    case(65535, &[255, 255]);
    case(65536, &[0, 0, 1]);
    case(u128::MAX, &[255; 16]);
  }
}
