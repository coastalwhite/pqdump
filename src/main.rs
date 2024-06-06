use std::fmt;
use std::io::{Write, stdout};

use parquet2::deserialize::{DefLevelsDecoder, HybridEncoded};
use parquet2::page::Page;
use parquet2::read::{get_page_iterator, read_metadata};

fn usage(w: &mut impl fmt::Write) -> fmt::Result {
    let bin = std::env::args().next().unwrap();

    writeln!(w, "Usage: {bin} <PATH>")?;

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PqDumpError {
    Usage,
    MultipleRowGroups,
    NoColumns,
}

impl fmt::Display for PqDumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PqDumpError as E;
        match self {
            E::Usage => {
                f.write_str("Invalid usage...")?;
                writeln!(f)?;
                usage(f)?;
            }
            E::MultipleRowGroups => {
                f.write_str("Multiple row groups given")?;
            },
            E::NoColumns => {
                f.write_str("No columns given")?;
            },
        }
        
        Ok(())
    }
}

impl std::error::Error for PqDumpError {}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut argv = std::env::args();

    argv.next().unwrap();

    let Some(path) = argv.next() else {
        return Err(PqDumpError::Usage.into());
    };

    let mut f = std::fs::File::open(path)?;
    let metadata = read_metadata(&mut f)?;

    if metadata.row_groups.len() > 1 {
        return Err(PqDumpError::MultipleRowGroups.into());
    }
    
    let row_group = metadata.row_groups.get(0).unwrap();

    if row_group.columns().is_empty() {
        return Err(PqDumpError::NoColumns.into());
    }

    let mut stdout = stdout();
    let stdout = &mut stdout;

    for column in row_group.columns() {
        let mut desc = column.descriptor().path_in_schema.iter();

        write!(stdout, "{}", desc.next().unwrap())?;
        for lvl in desc {
            write!(stdout, ".{}", lvl)?;
        }
        writeln!(stdout, ":")?;

        let pages = get_page_iterator(column, &mut f, None, vec![], 1024 * 1024)?;

        let mut decompress_buffer = vec![];
        for page in pages {
            let page = page?;
            let page = parquet2::read::decompress(page, &mut decompress_buffer)?;

            match page {
                Page::Data(page) => {
                    if page.num_values() == 0 {
                        continue;
                    }

                    let def_levels = DefLevelsDecoder::try_new(&page)?;

                    write!(stdout, "  dlvls: ")?;
                    match def_levels {
                        DefLevelsDecoder::Bitmap(decoder) => {
                            for subdecoder in decoder {
                                match subdecoder? {
                                    HybridEncoded::Bitmap(bs, mut length) => {
                                        let mut i = 0;
                                        loop {
                                            if length >= 8 {
                                                let b = bs[i];
                                                for s in 0..8 {
                                                    write!(stdout, "{} ", (b >> s) & 1)?;
                                                }

                                                i += 1;
                                                length -= 8;
                                            } else {
                                                let b = bs[i];
                                                for s in 0..length {
                                                    write!(stdout, "{} ", (b >> s) & 1)?;
                                                }
                                                break;
                                            }
                                        }
                                    },
                                    HybridEncoded::Repeated(v, length) => {
                                        for _ in 0..length {
                                            write!(stdout, "{} ", u16::from(v))?;
                                        }
                                    },
                                }
                            }
                        },
                        DefLevelsDecoder::Levels(decoder, max) => {
                            for lvl in decoder {
                                let lvl = lvl?;
                                write!(stdout, "{lvl} ")?;
                            }
                        },
                    }
                },
                Page::Dict(_) => {
                    // skip
                },
            }
        }
        writeln!(stdout)?;
    }

    Ok(())
}
