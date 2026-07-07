//! Stream quality selection against the multivariant playlist.

use crate::hls::m3u8::VariantStream;

/// Pick a variant by trying each name in `priority` in order.
///
/// Matching is case-insensitive and ignores parenthetical annotations in
/// variant names ("1080p60 (source)" matches "1080p60"). The special names
/// "best" and "worst" select by pixel area, then framerate, then bandwidth;
/// audio-only variants are only considered if nothing else exists.
pub fn select_variant<'a>(
    variants: &'a [VariantStream],
    priority: &[String],
) -> Option<&'a VariantStream> {
    if variants.is_empty() {
        return None;
    }

    for wanted in priority {
        let wanted = wanted.trim().to_lowercase();
        if wanted.is_empty() {
            continue;
        }
        match wanted.as_str() {
            "best" => {
                if let Some(v) = extreme_variant(variants, true) {
                    return Some(v);
                }
            }
            "worst" => {
                if let Some(v) = extreme_variant(variants, false) {
                    return Some(v);
                }
            }
            _ => {
                if let Some(v) = variants.iter().find(|v| {
                    v.name.to_lowercase() == wanted || v.normalized_name() == wanted
                }) {
                    return Some(v);
                }
            }
        }
    }

    None
}

fn weight(v: &VariantStream) -> (u64, u64, u64) {
    (
        v.pixels(),
        v.framerate.map(|f| f.round() as u64).unwrap_or(0),
        v.bandwidth,
    )
}

fn extreme_variant(variants: &[VariantStream], best: bool) -> Option<&VariantStream> {
    let candidates: Vec<&VariantStream> = variants.iter().filter(|v| v.pixels() > 0).collect();
    let pool: Vec<&VariantStream> = if candidates.is_empty() {
        variants.iter().collect()
    } else {
        candidates
    };
    if best {
        pool.into_iter().max_by_key(|v| weight(v))
    } else {
        pool.into_iter().min_by_key(|v| weight(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(name: &str, res: Option<(u32, u32)>, fps: Option<f64>, bw: u64) -> VariantStream {
        VariantStream {
            name: name.to_string(),
            uri: format!("https://example.com/{name}.m3u8"),
            bandwidth: bw,
            resolution: res,
            framerate: fps,
        }
    }

    fn typical() -> Vec<VariantStream> {
        vec![
            v(
                "1080p60 (source)",
                Some((1920, 1080)),
                Some(60.0),
                6_000_000,
            ),
            v("720p60", Some((1280, 720)), Some(60.0), 3_400_000),
            v("720p", Some((1280, 720)), Some(30.0), 2_400_000),
            v("480p", Some((852, 480)), Some(30.0), 1_400_000),
            v("360p", Some((640, 360)), Some(30.0), 630_000),
            v("160p", Some((284, 160)), Some(30.0), 230_000),
            v("audio_only", None, None, 160_000),
        ]
    }

    fn priority(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn exact_name_match() {
        let variants = typical();
        let selected = select_variant(&variants, &priority(&["720p60"])).unwrap();
        assert_eq!(selected.name, "720p60");
    }

    #[test]
    fn source_annotation_is_ignored_when_matching() {
        let variants = typical();
        let selected = select_variant(&variants, &priority(&["1080p60"])).unwrap();
        assert_eq!(selected.name, "1080p60 (source)");
    }

    #[test]
    fn priority_order_falls_through() {
        let variants = typical();
        // user's config: 1080p60,1080p,720p60,720p,480p,360p — no 1440p here
        let selected =
            select_variant(&variants, &priority(&["1440p", "1080p", "720p60"])).unwrap();
        assert_eq!(selected.name, "720p60");
    }

    #[test]
    fn best_and_worst() {
        let variants = typical();
        assert_eq!(
            select_variant(&variants, &priority(&["best"])).unwrap().name,
            "1080p60 (source)"
        );
        // worst ignores audio_only while video variants exist
        assert_eq!(
            select_variant(&variants, &priority(&["worst"])).unwrap().name,
            "160p"
        );
    }

    #[test]
    fn best_prefers_higher_framerate_at_same_resolution() {
        let variants = vec![
            v("720p", Some((1280, 720)), Some(30.0), 2_400_000),
            v("720p60", Some((1280, 720)), Some(60.0), 3_400_000),
        ];
        assert_eq!(
            select_variant(&variants, &priority(&["best"])).unwrap().name,
            "720p60"
        );
    }

    #[test]
    fn case_insensitive() {
        let variants = typical();
        let selected = select_variant(&variants, &priority(&["720P60"])).unwrap();
        assert_eq!(selected.name, "720p60");
    }

    #[test]
    fn no_match_returns_none() {
        let variants = typical();
        assert!(select_variant(&variants, &priority(&["4k"])).is_none());
    }

    #[test]
    fn audio_only_fallback_for_best() {
        let variants = vec![v("audio_only", None, None, 160_000)];
        assert_eq!(
            select_variant(&variants, &priority(&["best"])).unwrap().name,
            "audio_only"
        );
    }

    #[test]
    fn empty_variants() {
        assert!(select_variant(&[], &priority(&["best"])).is_none());
    }
}
