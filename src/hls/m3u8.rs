//! Hand-rolled HLS playlist parser with Twitch specifics:
//! prefetch segments, stitched-ad dateranges, EXTINF ad markers and
//! false-discontinuity fixes. Ported from the fork's `TwitchM3U8Parser`.

use std::collections::HashMap;

use chrono::{DateTime, Duration, FixedOffset};
use url::Url;

#[derive(Debug, Clone, PartialEq)]
pub struct DateRange {
    pub id: Option<String>,
    pub class: Option<String>,
    pub start_date: Option<DateTime<FixedOffset>>,
    pub end_date: Option<DateTime<FixedOffset>>,
    pub duration: Option<f64>,
    pub planned_duration: Option<f64>,
    /// X- client attributes (used for ad break duration logging).
    pub x: HashMap<String, String>,
}

impl DateRange {
    pub fn is_ad(&self) -> bool {
        self.class.as_deref() == Some("twitch-stitched-ad")
            || self
                .id
                .as_deref()
                .is_some_and(|id| id.starts_with("stitched-ad-"))
    }

    pub fn contains(&self, date: DateTime<FixedOffset>) -> bool {
        let Some(start) = self.start_date else {
            return false;
        };
        if let Some(end) = self.end_date {
            return start <= date && date < end;
        }
        if let Some(duration) = self.duration.or(self.planned_duration) {
            let end = start + Duration::microseconds((duration * 1_000_000.0) as i64);
            return start <= date && date < end;
        }
        start <= date
    }
}

#[derive(Debug, Clone)]
pub struct MediaSegment {
    pub uri: String,
    pub num: i64,
    pub duration: f64,
    pub title: Option<String>,
    pub date: Option<DateTime<FixedOffset>>,
    pub discontinuity: bool,
    pub map_uri: Option<String>,
    pub ad: bool,
    pub prefetch: bool,
}

#[derive(Debug, Default)]
pub struct MediaPlaylist {
    pub media_sequence: i64,
    pub targetduration: Option<f64>,
    pub is_endlist: bool,
    pub segments: Vec<MediaSegment>,
    pub dateranges_ads: Vec<DateRange>,
}

#[derive(Debug, Clone)]
pub struct VariantStream {
    pub name: String,
    pub uri: String,
    pub bandwidth: u64,
    pub resolution: Option<(u32, u32)>,
    pub framerate: Option<f64>,
}

impl VariantStream {
    pub fn pixels(&self) -> u64 {
        self.resolution
            .map(|(w, h)| w as u64 * h as u64)
            .unwrap_or(0)
    }

    /// Name with any parenthetical annotation removed: "1080p60 (source)" -> "1080p60"
    pub fn normalized_name(&self) -> String {
        normalize_quality_name(&self.name)
    }
}

#[derive(Debug, Default)]
pub struct MultivariantPlaylist {
    pub variants: Vec<VariantStream>,
}

pub fn normalize_quality_name(name: &str) -> String {
    match name.split_once('(') {
        Some((head, _)) => head.trim().to_lowercase(),
        None => name.trim().to_lowercase(),
    }
}

fn split_tag(line: &str) -> Option<(&str, &str)> {
    let rest = line.strip_prefix('#')?;
    match rest.split_once(':') {
        Some((tag, value)) => Some((tag, value.trim())),
        None => Some((rest, "")),
    }
}

/// Parse an HLS attribute list (KEY=VALUE,KEY="quoted value",...).
fn parse_attributes(value: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();
    let bytes = value.as_bytes();
    let mut pos = 0;

    while pos < bytes.len() {
        // skip whitespace and commas
        while pos < bytes.len() && (bytes[pos] == b',' || bytes[pos].is_ascii_whitespace()) {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        let key_start = pos;
        while pos < bytes.len() && bytes[pos] != b'=' {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        let key = value[key_start..pos].trim().to_string();
        pos += 1; // skip '='
        let val = if pos < bytes.len() && bytes[pos] == b'"' {
            pos += 1;
            let val_start = pos;
            while pos < bytes.len() && bytes[pos] != b'"' {
                pos += 1;
            }
            let val = value[val_start..pos].to_string();
            pos += 1; // skip closing quote
            val
        } else {
            let val_start = pos;
            while pos < bytes.len() && bytes[pos] != b',' {
                pos += 1;
            }
            value[val_start..pos].trim().to_string()
        };
        result.insert(key, val);
    }

    result
}

fn parse_iso8601(value: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(value).ok()
}

fn resolve_uri(base: Option<&Url>, uri: &str) -> String {
    match base {
        Some(base) => base
            .join(uri)
            .map(|u| u.to_string())
            .unwrap_or_else(|_| uri.to_string()),
        None => uri.to_string(),
    }
}

pub struct TwitchM3U8Parser {
    base_uri: Option<Url>,

    playlist: MediaPlaylist,
    dateranges_ads: Vec<DateRange>,

    // pending per-segment state
    extinf: Option<(f64, Option<String>)>,
    date: Option<DateTime<FixedOffset>>,
    discontinuity: bool,
    map_uri: Option<String>,
}

impl TwitchM3U8Parser {
    pub fn new(base_uri: Option<&str>) -> Self {
        Self {
            base_uri: base_uri.and_then(|u| Url::parse(u).ok()),
            playlist: MediaPlaylist::default(),
            dateranges_ads: Vec::new(),
            extinf: None,
            date: None,
            discontinuity: false,
            map_uri: None,
        }
    }

    pub fn parse(mut self, content: &str) -> MediaPlaylist {
        let mut lines = content.lines().filter(|l| !l.trim().is_empty());
        let Some(first) = lines.next() else {
            return self.playlist;
        };
        if !first.starts_with("#EXTM3U") {
            tracing::warn!("Malformed HLS playlist: missing #EXTM3U header");
            return self.playlist;
        }

        for line in lines {
            if line.starts_with('#') {
                if let Some((tag, value)) = split_tag(line) {
                    self.handle_tag(tag, value);
                }
            } else if self.extinf.is_some() {
                let uri = resolve_uri(self.base_uri.as_ref(), line.trim());
                let segment = self.take_segment(uri);
                self.playlist.segments.push(segment);
            }
        }

        // Assign media sequence numbers (prefetch segments included).
        let media_sequence = self.playlist.media_sequence;
        for (i, segment) in self.playlist.segments.iter_mut().enumerate() {
            segment.num = media_sequence + i as i64;
        }
        self.playlist.dateranges_ads = self.dateranges_ads;

        self.playlist
    }

    fn handle_tag(&mut self, tag: &str, value: &str) {
        match tag {
            "EXTINF" => {
                let (duration, title) = match value.split_once(',') {
                    Some((d, t)) => (
                        d.trim().parse::<f64>().unwrap_or(0.0),
                        if t.is_empty() {
                            None
                        } else {
                            Some(t.to_string())
                        },
                    ),
                    None => (value.trim().parse::<f64>().unwrap_or(0.0), None),
                };
                self.extinf = Some((duration, title));
            }
            "EXT-X-PROGRAM-DATE-TIME" => {
                self.date = parse_iso8601(value);
            }
            "EXT-X-DISCONTINUITY" => {
                self.discontinuity = true;
                self.map_uri = None;
            }
            "EXT-X-MAP" => {
                let attrs = parse_attributes(value);
                if let Some(uri) = attrs.get("URI") {
                    self.map_uri = Some(resolve_uri(self.base_uri.as_ref(), uri));
                }
            }
            "EXT-X-TARGETDURATION" => {
                self.playlist.targetduration = value.parse::<f64>().ok();
            }
            "EXT-X-MEDIA-SEQUENCE" => {
                self.playlist.media_sequence = value.parse::<i64>().unwrap_or(0);
            }
            "EXT-X-ENDLIST" => {
                self.playlist.is_endlist = true;
            }
            "EXT-X-DATERANGE" => {
                let attrs = parse_attributes(value);
                let daterange = DateRange {
                    id: attrs.get("ID").cloned(),
                    class: attrs.get("CLASS").cloned(),
                    start_date: attrs.get("START-DATE").and_then(|v| parse_iso8601(v)),
                    end_date: attrs.get("END-DATE").and_then(|v| parse_iso8601(v)),
                    duration: attrs.get("DURATION").and_then(|v| v.parse().ok()),
                    planned_duration: attrs.get("PLANNED-DURATION").and_then(|v| v.parse().ok()),
                    x: attrs
                        .iter()
                        .filter(|(k, _)| k.starts_with("X-"))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                };
                if daterange.is_ad() {
                    tracing::trace!("Advertisement daterange: {daterange:?}");
                    self.dateranges_ads.push(daterange);
                }
            }
            "EXT-X-TWITCH-LIVE-SEQUENCE"
                // Unset discontinuity state if the previous segment was not an
                // ad, as the following segment won't be an ad.
                if self
                    .playlist
                    .segments
                    .last()
                    .is_some_and(|last| !last.ad)
                => {
                    self.discontinuity = false;
                }
            "EXT-X-TWITCH-PREFETCH" => {
                self.handle_prefetch(value);
            }
            _ => {}
        }
    }

    fn is_segment_ad(&self, date: Option<DateTime<FixedOffset>>, title: Option<&str>) -> bool {
        if title.is_some_and(|t| t.contains("Amazon")) {
            return true;
        }
        if let Some(date) = date {
            return self.dateranges_ads.iter().any(|dr| dr.contains(date));
        }
        false
    }

    fn take_segment(&mut self, uri: String) -> MediaSegment {
        let (duration, title) = self.extinf.take().unwrap_or((0.0, None));
        let date = self.date.take();
        let mut discontinuity = std::mem::take(&mut self.discontinuity);

        let ad = self.is_segment_ad(date, title.as_deref());

        // Twitch sometimes incorrectly inserts discontinuity tags between
        // segments of the live content: clear them when neither the current
        // nor the previous segment is an ad.
        if discontinuity
            && !ad
            && self
                .playlist
                .segments
                .last()
                .is_some_and(|last| !last.ad)
        {
            discontinuity = false;
        }

        MediaSegment {
            uri,
            num: -1,
            duration,
            title,
            date,
            discontinuity,
            map_uri: self.map_uri.clone(),
            ad,
            prefetch: false,
        }
    }

    fn handle_prefetch(&mut self, value: &str) {
        let Some(last) = self.playlist.segments.last() else {
            return;
        };

        // Use the average duration of all segments for the first prefetch
        // segment (better than the last segment's duration when regular
        // segment durations vary); subsequent prefetch segments reuse it.
        let duration = if last.prefetch {
            last.duration
        } else {
            let segments = &self.playlist.segments;
            segments.iter().map(|s| s.duration).sum::<f64>() / segments.len() as f64
        };

        // Extrapolate the start time of the prefetch segment from the last
        // segment; needed for ad daterange checks.
        let Some(last_date) = last.date else {
            return;
        };
        let date = last_date + Duration::microseconds((last.duration * 1_000_000.0) as i64);

        // Always treat prefetch segments after a discontinuity as ad segments.
        // The discontinuity state is deliberately NOT reset here: the date
        // extrapolation may be inaccurate, so all following prefetch segments
        // stay flagged as ads after a discontinuity.
        let ad = self.discontinuity || self.is_segment_ad(Some(date), None);
        let discontinuity = ad != last.ad;

        let segment = MediaSegment {
            uri: resolve_uri(self.base_uri.as_ref(), value.trim()),
            num: -1,
            duration,
            title: None,
            date: Some(date),
            discontinuity,
            map_uri: last.map_uri.clone(),
            ad,
            prefetch: true,
        };
        self.playlist.segments.push(segment);
    }
}

pub fn parse_media_playlist(content: &str, base_uri: Option<&str>) -> MediaPlaylist {
    TwitchM3U8Parser::new(base_uri).parse(content)
}

/// Parse a multivariant (master) playlist into named variant streams.
///
/// The variant name comes from the matching `EXT-X-MEDIA` `VIDEO` rendition
/// (as in streamlink), with a fallback to the `IVS-NAME` STREAM-INF attribute
/// (Usher v2) and finally a "<height>p<fps>" pixel name.
pub fn parse_multivariant_playlist(content: &str, base_uri: Option<&str>) -> MultivariantPlaylist {
    let base = base_uri.and_then(|u| Url::parse(u).ok());
    let mut playlist = MultivariantPlaylist::default();

    // VIDEO group-id -> NAME
    let mut video_media: HashMap<String, String> = HashMap::new();
    let mut pending_streaminf: Option<HashMap<String, String>> = None;

    for line in content.lines().filter(|l| !l.trim().is_empty()) {
        if let Some((tag, value)) = split_tag(line) {
            match tag {
                "EXT-X-MEDIA" => {
                    let attrs = parse_attributes(value);
                    if attrs.get("TYPE").map(String::as_str) == Some("VIDEO")
                        && let (Some(group), Some(name)) =
                            (attrs.get("GROUP-ID"), attrs.get("NAME"))
                    {
                        video_media.insert(group.clone(), name.clone());
                    }
                }
                "EXT-X-STREAM-INF" => {
                    pending_streaminf = Some(parse_attributes(value));
                }
                _ => {}
            }
        } else if let Some(attrs) = pending_streaminf.take() {
            let uri = resolve_uri(base.as_ref(), line.trim());
            let resolution = attrs.get("RESOLUTION").and_then(|r| {
                let (w, h) = r.split_once('x')?;
                Some((w.parse().ok()?, h.parse().ok()?))
            });
            let framerate = attrs.get("FRAME-RATE").and_then(|f| f.parse::<f64>().ok());
            let bandwidth = attrs
                .get("BANDWIDTH")
                .and_then(|b| b.parse::<u64>().ok())
                .unwrap_or(0);

            let name = attrs
                .get("VIDEO")
                .and_then(|group| video_media.get(group).cloned())
                .or_else(|| attrs.get("IVS-NAME").cloned())
                .or_else(|| {
                    resolution.map(|(_, h)| match framerate {
                        Some(fps) if fps > 30.0 => format!("{h}p{}", fps.ceil() as u32),
                        _ => format!("{h}p"),
                    })
                });

            if let Some(name) = name {
                playlist.variants.push(VariantStream {
                    name,
                    uri,
                    bandwidth,
                    resolution,
                    framerate,
                });
            }
        }
    }

    playlist
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "https://example.com/hls/";

    fn seg_playlist(body: &str) -> MediaPlaylist {
        let content = format!("#EXTM3U\n#EXT-X-VERSION:3\n{body}");
        parse_media_playlist(&content, Some(BASE))
    }

    #[test]
    fn parses_basic_media_playlist() {
        let playlist = seg_playlist(
            "#EXT-X-TARGETDURATION:6\n\
             #EXT-X-MEDIA-SEQUENCE:100\n\
             #EXTINF:2.000,live\n\
             seg100.ts\n\
             #EXTINF:2.000,live\n\
             seg101.ts\n",
        );
        assert_eq!(playlist.media_sequence, 100);
        assert_eq!(playlist.targetduration, Some(6.0));
        assert_eq!(playlist.segments.len(), 2);
        assert_eq!(playlist.segments[0].num, 100);
        assert_eq!(playlist.segments[1].num, 101);
        assert_eq!(playlist.segments[0].uri, "https://example.com/hls/seg100.ts");
        assert_eq!(playlist.segments[0].title.as_deref(), Some("live"));
        assert!(!playlist.segments[0].ad);
        assert!(!playlist.is_endlist);
    }

    #[test]
    fn detects_ad_segments_by_daterange() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:50\n\
             #EXT-X-DATERANGE:ID=\"stitched-ad-1234\",CLASS=\"twitch-stitched-ad\",START-DATE=\"2026-01-01T00:00:10.000Z\",DURATION=4.000\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:08.000Z\n\
             #EXTINF:2.000,live\n\
             seg50.ts\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:10.000Z\n\
             #EXTINF:2.000,Amazon|123\n\
             ad1.ts\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:12.000Z\n\
             #EXTINF:2.000,\n\
             ad2.ts\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:14.000Z\n\
             #EXTINF:2.000,live\n\
             seg53.ts\n",
        );
        assert_eq!(playlist.dateranges_ads.len(), 1);
        let ads: Vec<bool> = playlist.segments.iter().map(|s| s.ad).collect();
        assert_eq!(ads, vec![false, true, true, false]);
    }

    #[test]
    fn detects_ad_by_amazon_title_without_daterange() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:0\n\
             #EXTINF:2.000,Amazon|its-an-ad\n\
             ad.ts\n\
             #EXTINF:2.000,live\n\
             seg.ts\n",
        );
        assert!(playlist.segments[0].ad);
        assert!(!playlist.segments[1].ad);
    }

    #[test]
    fn daterange_id_prefix_marks_ad() {
        let dr = DateRange {
            id: Some("stitched-ad-999".into()),
            class: None,
            start_date: None,
            end_date: None,
            duration: None,
            planned_duration: None,
            x: HashMap::new(),
        };
        assert!(dr.is_ad());
        let dr2 = DateRange {
            id: Some("something-else".into()),
            class: Some("twitch-stitched-ad".into()),
            ..dr.clone()
        };
        assert!(dr2.is_ad());
        let dr3 = DateRange {
            id: Some("other".into()),
            class: Some("other-class".into()),
            ..dr.clone()
        };
        assert!(!dr3.is_ad());
    }

    #[test]
    fn prefetch_segments_inherit_and_extrapolate() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:10\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z\n\
             #EXTINF:2.000,live\n\
             seg10.ts\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:02.000Z\n\
             #EXTINF:4.000,live\n\
             seg11.ts\n\
             #EXT-X-TWITCH-PREFETCH:https://example.com/hls/prefetch12.ts\n\
             #EXT-X-TWITCH-PREFETCH:https://example.com/hls/prefetch13.ts\n",
        );
        assert_eq!(playlist.segments.len(), 4);
        let p1 = &playlist.segments[2];
        let p2 = &playlist.segments[3];
        assert!(p1.prefetch && p2.prefetch);
        assert!(!p1.ad && !p2.ad);
        assert_eq!(p1.num, 12);
        assert_eq!(p2.num, 13);
        // first prefetch duration = average of regular segments (3.0)
        assert!((p1.duration - 3.0).abs() < 1e-9);
        // subsequent prefetch inherits it
        assert!((p2.duration - 3.0).abs() < 1e-9);
        // date extrapolated: last date (00:00:02) + last duration (4s)
        assert_eq!(
            p1.date.unwrap(),
            parse_iso8601("2026-01-01T00:00:06.000Z").unwrap()
        );
        assert!(!p1.discontinuity);
    }

    #[test]
    fn prefetch_after_discontinuity_is_ad() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:20\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z\n\
             #EXTINF:2.000,live\n\
             seg20.ts\n\
             #EXT-X-DISCONTINUITY\n\
             #EXT-X-TWITCH-PREFETCH:prefetch21.ts\n\
             #EXT-X-TWITCH-PREFETCH:prefetch22.ts\n",
        );
        assert_eq!(playlist.segments.len(), 3);
        let p1 = &playlist.segments[1];
        let p2 = &playlist.segments[2];
        assert!(p1.ad, "prefetch after discontinuity must be flagged as ad");
        assert!(p2.ad, "discontinuity state persists across prefetch segments");
        // ad transition sets the prefetch segment's discontinuity flag
        assert!(p1.discontinuity);
        assert!(!p2.discontinuity);
    }

    #[test]
    fn prefetch_in_ad_daterange_is_ad() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:30\n\
             #EXT-X-DATERANGE:ID=\"stitched-ad-1\",CLASS=\"twitch-stitched-ad\",START-DATE=\"2026-01-01T00:00:02.000Z\",PLANNED-DURATION=30.000\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z\n\
             #EXTINF:2.000,live\n\
             seg30.ts\n\
             #EXT-X-TWITCH-PREFETCH:prefetch31.ts\n",
        );
        let p = &playlist.segments[1];
        assert!(p.ad, "prefetch whose extrapolated date is in an ad daterange");
    }

    #[test]
    fn prefetch_without_date_is_skipped() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:1\n\
             #EXTINF:2.000,live\n\
             seg.ts\n\
             #EXT-X-TWITCH-PREFETCH:prefetch.ts\n",
        );
        assert_eq!(playlist.segments.len(), 1);
    }

    #[test]
    fn false_discontinuity_between_live_segments_is_cleared() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:5\n\
             #EXTINF:2.000,live\n\
             seg5.ts\n\
             #EXT-X-DISCONTINUITY\n\
             #EXTINF:2.000,live\n\
             seg6.ts\n",
        );
        assert!(!playlist.segments[1].discontinuity);
    }

    #[test]
    fn discontinuity_kept_on_ad_transition() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:5\n\
             #EXT-X-DATERANGE:CLASS=\"twitch-stitched-ad\",START-DATE=\"2026-01-01T00:00:02.000Z\",DURATION=10.000\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z\n\
             #EXTINF:2.000,live\n\
             seg5.ts\n\
             #EXT-X-DISCONTINUITY\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:02.000Z\n\
             #EXTINF:2.000,\n\
             ad.ts\n",
        );
        assert!(playlist.segments[1].ad);
        assert!(playlist.segments[1].discontinuity);
    }

    #[test]
    fn live_sequence_tag_clears_discontinuity_after_live_segment() {
        let playlist = seg_playlist(
            "#EXT-X-MEDIA-SEQUENCE:5\n\
             #EXTINF:2.000,live\n\
             seg5.ts\n\
             #EXT-X-DISCONTINUITY\n\
             #EXT-X-TWITCH-LIVE-SEQUENCE:7\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z\n\
             #EXTINF:2.000,live\n\
             seg6.ts\n\
             #EXT-X-TWITCH-PREFETCH:prefetch7.ts\n",
        );
        assert!(!playlist.segments[1].discontinuity);
        // prefetch after the cleared discontinuity is not flagged as ad
        assert!(!playlist.segments[2].ad);
    }

    #[test]
    fn endlist_detected() {
        let playlist = seg_playlist(
            "#EXTINF:2.000,live\nseg.ts\n#EXT-X-ENDLIST\n",
        );
        assert!(playlist.is_endlist);
    }

    #[test]
    fn parses_multivariant() {
        let content = "#EXTM3U\n\
            #EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"chunked\",NAME=\"1080p60 (source)\",AUTOSELECT=YES,DEFAULT=YES\n\
            #EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080,CODECS=\"avc1.64002A,mp4a.40.2\",VIDEO=\"chunked\",FRAME-RATE=60.000\n\
            https://example.com/chunked.m3u8\n\
            #EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"720p60\",NAME=\"720p60\",AUTOSELECT=YES,DEFAULT=YES\n\
            #EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720,VIDEO=\"720p60\",FRAME-RATE=60.000\n\
            https://example.com/720p60.m3u8\n\
            #EXT-X-MEDIA:TYPE=VIDEO,GROUP-ID=\"audio_only\",NAME=\"audio_only\",AUTOSELECT=NO,DEFAULT=NO\n\
            #EXT-X-STREAM-INF:BANDWIDTH=160000,CODECS=\"mp4a.40.2\",VIDEO=\"audio_only\"\n\
            https://example.com/audio.m3u8\n";
        let playlist = parse_multivariant_playlist(content, None);
        assert_eq!(playlist.variants.len(), 3);
        assert_eq!(playlist.variants[0].name, "1080p60 (source)");
        assert_eq!(playlist.variants[0].normalized_name(), "1080p60");
        assert_eq!(playlist.variants[0].resolution, Some((1920, 1080)));
        assert_eq!(playlist.variants[1].name, "720p60");
        assert_eq!(playlist.variants[2].name, "audio_only");
        assert_eq!(playlist.variants[2].pixels(), 0);
    }

    #[test]
    fn multivariant_ivs_name_fallback() {
        let content = "#EXTM3U\n\
            #EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080,IVS-NAME=\"1080p60\",FRAME-RATE=60.000\n\
            https://example.com/a.m3u8\n\
            #EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=852x480,FRAME-RATE=30.000\n\
            https://example.com/b.m3u8\n";
        let playlist = parse_multivariant_playlist(content, None);
        assert_eq!(playlist.variants[0].name, "1080p60");
        // pixel-name fallback
        assert_eq!(playlist.variants[1].name, "480p");
    }

    #[test]
    fn attribute_parser_handles_quotes_and_commas() {
        let attrs = parse_attributes(
            "ID=\"stitched-ad-1\",CLASS=\"twitch-stitched-ad\",X-TV-TWITCH-AD-POD-FILLED-DURATION=\"30.5\",DURATION=29.933",
        );
        assert_eq!(attrs.get("ID").unwrap(), "stitched-ad-1");
        assert_eq!(attrs.get("CLASS").unwrap(), "twitch-stitched-ad");
        assert_eq!(attrs.get("DURATION").unwrap(), "29.933");
        assert_eq!(
            attrs.get("X-TV-TWITCH-AD-POD-FILLED-DURATION").unwrap(),
            "30.5"
        );
    }

    #[test]
    fn daterange_contains() {
        let start = parse_iso8601("2026-01-01T00:00:00.000Z").unwrap();
        let mk = |end: Option<&str>, duration: Option<f64>, planned: Option<f64>| DateRange {
            id: None,
            class: None,
            start_date: Some(start),
            end_date: end.map(|e| parse_iso8601(e).unwrap()),
            duration,
            planned_duration: planned,
            x: HashMap::new(),
        };
        let at = |s: &str| parse_iso8601(s).unwrap();

        let with_end = mk(Some("2026-01-01T00:00:10.000Z"), None, None);
        assert!(with_end.contains(at("2026-01-01T00:00:05.000Z")));
        assert!(!with_end.contains(at("2026-01-01T00:00:10.000Z")));

        let with_duration = mk(None, Some(10.0), None);
        assert!(with_duration.contains(at("2026-01-01T00:00:09.000Z")));
        assert!(!with_duration.contains(at("2026-01-01T00:00:10.000Z")));

        let with_planned = mk(None, None, Some(10.0));
        assert!(with_planned.contains(at("2026-01-01T00:00:09.000Z")));

        let open_ended = mk(None, None, None);
        assert!(open_ended.contains(at("2026-01-01T01:00:00.000Z")));
        assert!(!open_ended.contains(at("2025-12-31T23:59:59.000Z")));
    }
}
