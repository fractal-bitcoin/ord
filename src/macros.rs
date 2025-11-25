// This macro has been removed for RocksDB compatibility
// RocksDB doesn't use table definitions like redb

// This macro has been removed for RocksDB compatibility
// RocksDB doesn't use multimap table definitions like redb

#[macro_export]
macro_rules! tprintln {
  ($($arg:tt)*) => {
    if cfg!(test) {
      eprint!("==> ");
      eprintln!($($arg)*);
    }
  };
}

#[macro_export]
macro_rules! assert_regex_match {
  ($value:expr, $pattern:expr $(,)?) => {
    let regex = Regex::new(&format!("^(?s){}$", $pattern)).unwrap();
    let string = $value.to_string();

    if !regex.is_match(string.as_ref()) {
      eprintln!("Regex did not match:");
      pretty_assert_eq!(regex.as_str(), string);
    }
  };
}

#[macro_export]
macro_rules! assert_matches {
  ($expression:expr, $( $pattern:pat_param )|+ $( if $guard:expr )? $(,)?) => {
    match $expression {
      $( $pattern )|+ $( if $guard )? => {}
      left => panic!(
        "assertion failed: (left ~= right)\n  left: `{:?}`\n right: `{}`",
        left,
        stringify!($($pattern)|+ $(if $guard)?)
      ),
    }
  }
}
