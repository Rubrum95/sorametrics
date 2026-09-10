//! Prometheus text exposition parser — the subset Iroha's `/metrics`
//! emits (gauges, counters, histograms). `# HELP` / `# TYPE` lines are
//! dropped; only samples are kept. Port of `minamoto/prom_parser.js`.

use std::collections::BTreeMap;

/// One sample line: `name{labels} value [timestamp]`.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    /// Metric name.
    pub name: String,
    /// Label set (sorted by key).
    pub labels: BTreeMap<String, String>,
    /// Sample value. `NaN` / `±Inf` lines are skipped at parse time.
    pub value: f64,
}

/// Parses an exposition body. Malformed lines are skipped, matching the
/// Node parser (a scrape with one odd line still yields the rest).
pub fn parse(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, labels_raw, rest) = match line.find('{') {
            Some(open) => {
                let Some(close) = line.rfind('}') else {
                    continue;
                };
                if close < open {
                    continue;
                }
                (
                    line[..open].trim(),
                    &line[open + 1..close],
                    line[close + 1..].trim(),
                )
            }
            None => {
                let Some(sp) = line.find(' ') else {
                    continue;
                };
                (line[..sp].trim(), "", line[sp + 1..].trim())
            }
        };
        if name.is_empty() {
            continue;
        }
        let Some(value_tok) = rest.split_whitespace().next() else {
            continue;
        };
        let Some(value) = parse_value(value_tok) else {
            continue;
        };
        out.push(Sample {
            name: name.to_string(),
            labels: parse_labels(labels_raw),
            value,
        });
    }
    out
}

fn parse_value(tok: &str) -> Option<f64> {
    if matches!(tok, "NaN" | "+Inf" | "-Inf") {
        return None;
    }
    tok.parse::<f64>().ok().filter(|v| v.is_finite())
}

/// Parses the body inside `{…}`: `key="value",…` with `\"`, `\\` and
/// `\n` escapes inside values.
fn parse_labels(raw: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let chars: Vec<char> = raw.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        while i < chars.len() && (chars[i] == ',' || chars[i] == ' ') {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        let key_start = i;
        while i < chars.len() && chars[i] != '=' {
            i += 1;
        }
        let key: String = chars[key_start..i]
            .iter()
            .collect::<String>()
            .trim()
            .to_string();
        if i >= chars.len() {
            break;
        }
        i += 1;
        if i >= chars.len() || chars[i] != '"' {
            break;
        }
        i += 1;
        let mut val = String::new();
        while i < chars.len() && chars[i] != '"' {
            if chars[i] == '\\' && i + 1 < chars.len() {
                match chars[i + 1] {
                    '"' | '\\' => {
                        val.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    'n' => {
                        val.push('\n');
                        i += 2;
                        continue;
                    }
                    _ => {}
                }
            }
            val.push(chars[i]);
            i += 1;
        }
        i += 1;
        if !key.is_empty() {
            out.insert(key, val);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gauges_counters_and_labels() {
        let text = "# HELP blocks Committed\n# TYPE blocks gauge\nblocks 438\nsumeragi_post_to_peer_total{peer=\"ea01\",lane=\"0\"} 12 1700000000\n";
        let s = parse(text);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].name, "blocks");
        assert_eq!(s[0].value, 438.0);
        assert_eq!(s[1].labels["peer"], "ea01");
        assert_eq!(s[1].labels["lane"], "0");
        assert_eq!(s[1].value, 12.0);
    }

    #[test]
    fn skips_nan_inf_and_garbage() {
        let text = "a NaN\nb +Inf\nc{x=\"1\" 3\nd\n\ne 2.5\n";
        let s = parse(text);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].name, "e");
        assert_eq!(s[0].value, 2.5);
    }

    #[test]
    fn label_escapes() {
        let text = "m{msg=\"a \\\"q\\\" \\\\ z\\nend\"} 1\n";
        let s = parse(text);
        assert_eq!(s[0].labels["msg"], "a \"q\" \\ z\nend");
    }

    #[test]
    fn empty_label_set_is_empty_map() {
        let s = parse("m{} 7\n");
        assert!(s[0].labels.is_empty());
        assert_eq!(s[0].value, 7.0);
    }
}
