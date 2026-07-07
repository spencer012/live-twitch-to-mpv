//! Usher multivariant playlist URL builder.

use url::Url;

use crate::twitch::api::AccessToken;

pub const PLAYER_REFERER: &str = "https://player.twitch.tv";

#[derive(Clone)]
pub struct UsherService {
    supported_codecs: Vec<String>,
}

impl UsherService {
    pub fn new(supported_codecs: &[String]) -> Self {
        let supported_codecs = if supported_codecs.is_empty() {
            vec!["h264".to_string()]
        } else {
            supported_codecs.to_vec()
        };
        Self { supported_codecs }
    }

    /// Build the channel HLS multivariant playlist URL.
    ///
    /// `strip_parent_domains` mirrors the fork's adblock behavior: the token
    /// may embed a `parent_domains` value which is dropped from the final URL
    /// query to avoid fake ads (the parameter only appears if present).
    pub fn channel_url(
        &self,
        channel: &str,
        token: &AccessToken,
        strip_parent_domains: bool,
    ) -> Url {
        let channel = channel.to_lowercase();
        let mut url = Url::parse(&format!(
            "https://usher.ttvnw.net/api/v2/channel/hls/{channel}.m3u8"
        ))
        .expect("static usher URL is valid");

        // Random-ish cache-busting value in 0..=999999 (as the web player sends).
        let p: u32 = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0))
            % 1_000_000;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("platform", "web")
                .append_pair("p", &p.to_string())
                .append_pair("allow_source", "true")
                .append_pair("allow_audio_only", "true")
                .append_pair("playlist_include_framerate", "true")
                .append_pair("supported_codecs", &self.supported_codecs.join(","))
                .append_pair("fast_bread", "true")
                .append_pair("sig", &token.signature)
                .append_pair("token", &token.value);
        }

        if strip_parent_domains {
            strip_query_param(&mut url, "parent_domains");
        }

        url
    }
}

fn strip_query_param(url: &mut Url, param: &str) {
    let filtered: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| k != param)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    url.query_pairs_mut().clear().extend_pairs(filtered);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token() -> AccessToken {
        AccessToken {
            signature: "sig".into(),
            value: "{\"channel\":\"test\"}".into(),
        }
    }

    #[test]
    fn builds_channel_url() {
        let usher = UsherService::new(&["av1".into(), "h265".into(), "h264".into()]);
        let url = usher.channel_url("TestChannel", &token(), false);
        assert!(
            url.as_str()
                .starts_with("https://usher.ttvnw.net/api/v2/channel/hls/testchannel.m3u8?")
        );
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let get = |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get("platform").as_deref(), Some("web"));
        assert_eq!(get("fast_bread").as_deref(), Some("true"));
        assert_eq!(get("supported_codecs").as_deref(), Some("av1,h265,h264"));
        assert_eq!(get("sig").as_deref(), Some("sig"));
        assert_eq!(get("token").as_deref(), Some("{\"channel\":\"test\"}"));
        let p: u32 = get("p").unwrap().parse().unwrap();
        assert!(p <= 999_999);
    }

    #[test]
    fn strips_parent_domains() {
        let mut url = Url::parse("https://example.com/x?a=1&parent_domains=twitch.tv&b=2").unwrap();
        strip_query_param(&mut url, "parent_domains");
        assert_eq!(url.query(), Some("a=1&b=2"));
    }
}
