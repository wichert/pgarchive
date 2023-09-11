use std::io;
use std::num::ParseIntError;
use std::string::String;

pub type Oid = u64;

#[derive(PartialEq, Debug)]
pub enum Offset {
    Unknown,
    PosNotSet,
    PosSet(u64),
    NoData,
}

pub struct ReadConfig {
    pub int_size: usize,
    pub offset_size: usize,
}

impl ReadConfig {
    pub fn new() -> ReadConfig {
        ReadConfig {
            int_size: 0,
            offset_size: 0,
        }
    }

    pub fn read_byte(&self, f: &mut (impl io::Read + ?Sized)) -> io::Result<u8> {
        let mut buffer: [u8; 1] = [0];
        f.read_exact(&mut buffer)?;
        Ok(buffer[0])
    }

    pub fn read_int(&self, f: &mut (impl io::Read + ?Sized)) -> io::Result<i64> {
        if self.int_size == 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "integer size unknown"));
        }

        let mut buffer = Vec::with_capacity(self.int_size + 1);
        buffer.resize(self.int_size + 1, 0);
        f.read_exact(buffer.as_mut_slice())?;
        let is_negative = buffer[0] != 0;
        let mut result: i64 = 0;

        for i in 0..self.int_size {
            result += (buffer[i + 1] as i64) << (i * 8);
        }

        Ok(if is_negative { -result } else { result })
    }

    pub fn read_string(&self, f: &mut (impl io::Read + ?Sized)) -> io::Result<String> {
        let length = self.read_int(f)?;
        if length == -1 {
            return Ok(String::new());
        }
        if length < 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "invalid string length",
            ));
        }
        let mut buffer = Vec::with_capacity(length as usize);
        buffer.resize(length as usize, 0);
        f.read_exact(buffer.as_mut_slice())?;
        let s = String::from_utf8(buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(s)
    }

    pub fn read_int_bool(&self, f: &mut (impl io::Read + ?Sized)) -> io::Result<bool> {
        self.read_int(f).map(|v| v != 0)
    }

    pub fn read_string_bool(&self, f: &mut (impl io::Read + ?Sized)) -> io::Result<bool> {
        self.read_string(f).map(|v| v == "true")
    }

    pub fn read_oid(&self, f: &mut (impl io::Read + ?Sized)) -> io::Result<Oid> {
        let v = self.read_string(f)?;
        Oid::from_str_radix(v.as_str(), 10)
            .map_err(|e: ParseIntError| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn read_offset(&self, f: &mut (impl io::Read + ?Sized)) -> io::Result<Offset> {
        if self.offset_size == 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "offset size unknown"));
        }

        let mut buffer = Vec::with_capacity(self.offset_size + 1);
        buffer.resize(self.offset_size + 1, 0);
        f.read_exact(buffer.as_mut_slice())?;

        match buffer[0] {
            0 => Ok(Offset::Unknown),
            1 => Ok(Offset::PosNotSet),
            2 => {
                let mut offset: u64 = 0;
                for i in 0..self.offset_size {
                    offset |= (buffer[i + 1] as u64) << (i * 8);
                }
                Ok(Offset::PosSet(offset))
            }
            3 => Ok(Offset::NoData),
            _ => Err(io::Error::new(io::ErrorKind::Other, "invalid offset type")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_byte() -> Result<(), io::Error> {
        let cfg = ReadConfig::new();

        // valid
        let mut input: &[u8] = b"\x42";
        assert_eq!(cfg.read_byte(&mut input)?, 0x42);

        // not enough data
        input = b"";
        assert!(cfg.read_byte(&mut input).is_err());
        Ok(())
    }

    #[test]
    fn read_int() -> Result<(), io::Error> {
        let mut cfg = ReadConfig::new();

        // no int_size set
        let mut input: &[u8] = b"\x01\x02\x03\x04";
        assert!(cfg.read_int(&mut input).is_err());

        // positive int
        cfg.int_size = 2;
        input = b"\x00\x01\x02";
        assert_eq!(cfg.read_int(&mut input)?, 0x0201);

        // negative int
        input = b"\x01\x01\x02";
        assert_eq!(cfg.read_int(&mut input)?, -0x0201);

        // not enough data
        input = b"\x00";
        assert!(cfg.read_int(&mut input).is_err());

        Ok(())
    }

    #[test]
    fn read_string() -> Result<(), io::Error> {
        let mut cfg: ReadConfig = ReadConfig::new();

        // no int_size set
        let mut input: &[u8] = b"\x01\x02\x03\x04";
        assert!(cfg.read_string(&mut input).is_err());

        // empty string
        cfg.int_size = 2;
        input = b"\x01\x01\x00";
        assert_eq!(cfg.read_string(&mut input)?, "");

        // negative length
        input = b"\x01\x02\x00";
        assert!(cfg.read_string(&mut input).is_err());

        // valid string
        input = b"\x00\x0d\x00hello, world!";
        assert_eq!(cfg.read_string(&mut input)?, "hello, world!");

        // not enough data
        input = b"\x00";
        assert!(cfg.read_string(&mut input).is_err());

        Ok(())
    }

    #[test]
    fn read_int_bool() -> Result<(), io::Error> {
        let mut cfg: ReadConfig = ReadConfig::new();

        // no int_size set
        let mut input: &[u8] = b"\x01\x01\x00";
        assert!(cfg.read_int_bool(&mut input).is_err());

        // postive value
        cfg.int_size = 2;
        input = b"\x01\x01\x00";
        assert_eq!(cfg.read_int_bool(&mut input)?, true);

        // negative value
        input = b"\x01\x02\x00";
        assert_eq!(cfg.read_int_bool(&mut input)?, true);

        // zero is false
        input = b"\x00\x00\x00";
        assert_eq!(cfg.read_int_bool(&mut input)?, false);

        // not enough data
        input = b"\x00";
        assert!(cfg.read_int_bool(&mut input).is_err());

        Ok(())
    }

    #[test]
    fn read_string_bool() -> Result<(), io::Error> {
        let mut cfg: ReadConfig = ReadConfig::new();

        // no int_size set
        let mut input: &[u8] = b"\x00\x04\x00true";
        assert!(cfg.read_string_bool(&mut input).is_err());

        // true
        cfg.int_size = 2;
        input = b"\x00\x04\x00true";
        assert_eq!(cfg.read_string_bool(&mut input)?, true);

        // false
        input = b"\x00\x05\x00false";
        assert_eq!(cfg.read_string_bool(&mut input)?, false);

        // other text
        input = b"\x00\x04\x00oops";
        assert_eq!(cfg.read_string_bool(&mut input)?, false);

        // not enough data
        input = b"\x00";
        assert!(cfg.read_string_bool(&mut input).is_err());

        Ok(())
    }

    #[test]
    fn read_oid() -> Result<(), io::Error> {
        let mut cfg: ReadConfig = ReadConfig::new();

        // no int_size set
        let mut input: &[u8] = b"\x01\x02\x03\x04";
        assert!(cfg.read_oid(&mut input).is_err());

        // positive number
        cfg.int_size = 2;
        input = b"\x00\x04\x001234";
        assert_eq!(cfg.read_oid(&mut input)?, 1234);

        // negative number
        input = b"\x00\x05\x00-1234";
        assert!(cfg.read_oid(&mut input).is_err());

        // bad number
        input = b"\x00\x05\x00x1234";
        assert!(cfg.read_oid(&mut input).is_err());

        // not enough data
        input = b"\x00";
        assert!(cfg.read_oid(&mut input).is_err());

        Ok(())
    }

    #[test]
    fn read_offset() -> Result<(), io::Error> {
        let mut cfg: ReadConfig = ReadConfig::new();

        // no offset_size set
        let mut input: &[u8] = b"\x01\x02\x03\x04";
        assert!(cfg.read_offset(&mut input).is_err());

        // valid offset, no flag
        cfg.offset_size = 2;
        input = b"\x00\x01\x02";
        assert_eq!(cfg.read_offset(&mut input)?, Offset::Unknown);

        // valid offset, pos-not-set flag
        input = b"\x01\x01\x02";
        assert_eq!(cfg.read_offset(&mut input)?, Offset::PosNotSet);

        // valid offset, pos-set flag
        input = b"\x02\x01\x02";
        assert_eq!(cfg.read_offset(&mut input)?, Offset::PosSet(513));

        // valid offset, no-data flag
        input = b"\x03\x01\x02";
        assert_eq!(cfg.read_offset(&mut input)?, Offset::NoData);

        // not enough data
        input = b"\x00";
        assert!(cfg.read_offset(&mut input).is_err());

        Ok(())
    }
}
