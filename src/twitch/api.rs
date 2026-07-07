//! Twitch GQL API client (persisted queries only, no client-integrity flow).

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Value, json};

pub const CLIENT_ID: &str = "kimne78kx3ncx6brgo4mv6wki5h1ko";
const GQL_URL: &str = "https://gql.twitch.tv/gql";

const HASH_PLAYBACK_ACCESS_TOKEN: &str =
    "ed230aa1e33e07eebb8928504583da78a5173989fadfb1ac94be06a04f3cdbe9";
const HASH_CHANNEL_SHELL: &str = "fea4573a7bf2644f5b3f2cbbdcbee0d17312e48d2e55f080589d053aad353f11";
const HASH_STREAM_METADATA: &str =
    "b57f9b910f8cd1a4659d894fe7550ccc81ec9052c01e438b290fd66a040b9b93";

#[derive(Debug, Clone)]
pub struct AccessToken {
    pub signature: String,
    pub value: String,
}

#[derive(Debug, Clone, Default)]
pub struct ChannelMetadata {
    pub display_name: Option<String>,
    pub stream_id: Option<String>,
    pub game: Option<String>,
    pub title: Option<String>,
}

impl ChannelMetadata {
    pub fn is_live(&self) -> bool {
        self.stream_id.is_some()
    }
}

/// Outcome of a PlaybackAccessToken request.
#[derive(Debug)]
pub enum TokenResult {
    Token(AccessToken),
    /// The API responded successfully but with a null token:
    /// the channel is offline or doesn't exist.
    Offline,
    /// The API returned an error message (auth failure, integrity check, ...).
    Error(String),
}

#[derive(Clone)]
pub struct TwitchApi {
    client: reqwest::Client,
    headers: HeaderMap,
}

impl TwitchApi {
    pub fn new(client: reqwest::Client, extra_headers: &BTreeMap<String, String>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert("Client-ID", HeaderValue::from_static(CLIENT_ID));
        for (key, value) in extra_headers {
            let name: HeaderName = key
                .parse()
                .with_context(|| format!("invalid API header name: {key}"))?;
            let value = HeaderValue::from_str(value)
                .with_context(|| format!("invalid API header value for {key}"))?;
            headers.insert(name, value);
        }
        Ok(Self { client, headers })
    }

    fn persisted_query(operation: &str, hash: &str, variables: Value) -> Value {
        json!({
            "operationName": operation,
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": hash,
                },
            },
            "variables": variables,
        })
    }

    async fn call(&self, body: &Value) -> Result<(u16, Value)> {
        let resp = self
            .client
            .post(GQL_URL)
            .headers(self.headers.clone())
            .json(body)
            .send()
            .await
            .context("GQL request failed")?;
        let status = resp.status().as_u16();
        let value: Value = resp.json().await.context("invalid GQL JSON response")?;
        Ok((status, value))
    }

    /// Request a streaming access token for a live channel.
    ///
    /// The main token uses `player_type="popout"` / `platform="site"`;
    /// the adblock managers request other combinations.
    pub async fn access_token(
        &self,
        channel: &str,
        player_type: &str,
        platform: &str,
    ) -> Result<TokenResult> {
        let query = Self::persisted_query(
            "PlaybackAccessToken",
            HASH_PLAYBACK_ACCESS_TOKEN,
            json!({
                "isLive": true,
                "login": channel,
                "isVod": false,
                "vodID": "",
                "playerType": player_type,
                "platform": platform,
            }),
        );

        let (status, value) = self.call(&query).await?;
        if ![200, 400, 401, 403].contains(&status) {
            anyhow::bail!("unexpected GQL response status: {status}");
        }

        Ok(parse_token_response(&value))
    }

    /// Fetch channel metadata via batched ChannelShell + StreamMetadata queries.
    /// Missing fields are left as `None` (e.g. when the channel is offline).
    pub async fn metadata_channel(&self, channel: &str) -> Result<ChannelMetadata> {
        let queries = json!([
            Self::persisted_query(
                "ChannelShell",
                HASH_CHANNEL_SHELL,
                json!({ "login": channel }),
            ),
            Self::persisted_query(
                "StreamMetadata",
                HASH_STREAM_METADATA,
                json!({ "channelLogin": channel, "includeIsDJ": true }),
            ),
        ]);

        let (status, value) = self.call(&queries).await?;
        if status != 200 {
            anyhow::bail!("metadata request failed with status {status}");
        }

        Ok(parse_metadata_response(&value))
    }
}

fn parse_token_response(value: &Value) -> TokenResult {
    // {"errors": [{"message": ...}]}
    if let Some(message) = value
        .pointer("/errors/0/message")
        .and_then(Value::as_str)
    {
        return TokenResult::Error(message.to_string());
    }
    // {"error": ..., "message": ...}
    if let (Some(error), Some(message)) = (
        value.get("error").and_then(Value::as_str),
        value.get("message").and_then(Value::as_str),
    ) {
        return TokenResult::Error(format!("{error}: {message}"));
    }

    let token = value.pointer("/data/streamPlaybackAccessToken");
    match token {
        Some(Value::Null) | None => TokenResult::Offline,
        Some(token) => {
            let signature = token.get("signature").and_then(Value::as_str);
            let value = token.get("value").and_then(Value::as_str);
            match (signature, value) {
                (Some(signature), Some(value)) => TokenResult::Token(AccessToken {
                    signature: signature.to_string(),
                    value: value.to_string(),
                }),
                _ => TokenResult::Error("malformed access token response".to_string()),
            }
        }
    }
}

fn parse_metadata_response(value: &Value) -> ChannelMetadata {
    let as_string = |v: Option<&Value>| v.and_then(Value::as_str).map(str::to_string);
    ChannelMetadata {
        display_name: as_string(value.pointer("/0/data/userOrError/displayName")),
        stream_id: as_string(value.pointer("/1/data/user/stream/id")),
        game: as_string(value.pointer("/1/data/user/stream/game/name")),
        title: as_string(value.pointer("/1/data/user/lastBroadcast/title")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_response_ok() {
        let value = json!({
            "data": {
                "streamPlaybackAccessToken": {
                    "signature": "sig123",
                    "value": "{\"channel\":\"x\"}",
                },
            },
        });
        match parse_token_response(&value) {
            TokenResult::Token(t) => {
                assert_eq!(t.signature, "sig123");
                assert_eq!(t.value, "{\"channel\":\"x\"}");
            }
            other => panic!("expected token, got {other:?}"),
        }
    }

    #[test]
    fn token_response_offline() {
        let value = json!({ "data": { "streamPlaybackAccessToken": null } });
        assert!(matches!(parse_token_response(&value), TokenResult::Offline));
    }

    #[test]
    fn token_response_errors_array() {
        let value = json!({ "errors": [{ "message": "failed integrity check" }] });
        match parse_token_response(&value) {
            TokenResult::Error(message) => assert_eq!(message, "failed integrity check"),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn token_response_error_object() {
        let value = json!({ "error": "Unauthorized", "message": "bad token" });
        match parse_token_response(&value) {
            TokenResult::Error(message) => assert_eq!(message, "Unauthorized: bad token"),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn metadata_response() {
        let value = json!([
            { "data": { "userOrError": { "displayName": "SomeStreamer" } } },
            { "data": { "user": {
                "lastBroadcast": { "title": "playing games" },
                "stream": { "id": "42", "game": { "name": "Tetris" } },
            } } },
        ]);
        let meta = parse_metadata_response(&value);
        assert_eq!(meta.display_name.as_deref(), Some("SomeStreamer"));
        assert_eq!(meta.stream_id.as_deref(), Some("42"));
        assert_eq!(meta.game.as_deref(), Some("Tetris"));
        assert_eq!(meta.title.as_deref(), Some("playing games"));
        assert!(meta.is_live());
    }

    #[test]
    fn metadata_response_offline() {
        let value = json!([
            { "data": { "userOrError": { "displayName": "SomeStreamer" } } },
            { "data": { "user": { "lastBroadcast": { "title": "old title" }, "stream": null } } },
        ]);
        let meta = parse_metadata_response(&value);
        assert!(!meta.is_live());
        assert_eq!(meta.title.as_deref(), Some("old title"));
    }
}
