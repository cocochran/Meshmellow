//! Chat templates: how the front end turns OpenAI chat messages into the prompt string the
//! tokenizing stage tokenizes.
//!
//! [`ChatRenderer::load`] prepares a manifest's [`ChatTemplate`] once, at startup:
//!
//! - `chatml` needs no preparation.
//! - A pinned Jinja template is read from disk, its SHA-256 checked against the manifest, and
//!   compiled with [`minijinja`]; any failure refuses to start. It renders as Hugging Face's
//!   `apply_chat_template` and david-codex's ring demo do: `trim_blocks` and `lstrip_blocks`
//!   on, Python string methods (`.strip()`, `.startswith()`, `.split()`, ...) through
//!   `minijinja-contrib`'s pycompat, and a context of `messages`, `add_generation_prompt = true`,
//!   `enable_thinking` from the manifest, `tools = none` and a `raise_exception(msg)` function.
//!
//! [`ChatRenderer::render`] then runs per request; a template error (including
//! `raise_exception`) is a [`RenderError`], which the front end answers with HTTP 400.

use std::path::Path;

use anyhow::{Context, bail};
use minijinja::syntax::SyntaxConfig;
use minijinja::{Environment, Error, ErrorKind, Value, context};
use sha2::{Digest, Sha256};

use crate::shard::manifest::{ChatTemplate, PinnedTemplate, same_sha256};
use crate::shard::wire;

/// Name the pinned template is registered under; template errors cite it.
const TEMPLATE_NAME: &str = "chat_template";

/// The function Hugging Face chat templates call to refuse a conversation.
const RAISE_EXCEPTION: &str = "raise_exception";

/// ChatML framing around each message and before the assistant's turn.
const CHATML_START: &str = "<|im_start|>";
const CHATML_END: &str = "<|im_end|>\n";
const CHATML_ASSISTANT: &str = "<|im_start|>assistant\n";

/// One chat message as the template sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatMessage {
    /// `system`, `user`, `assistant`, ... passed through unchanged.
    pub role: String,
    /// The message text (text parts already joined).
    pub content: String,
}

/// The template rejected or failed on a request: a client error, not a server fault.
#[derive(Debug, thiserror::Error)]
#[error("the chat template could not render this request: {0}")]
pub struct RenderError(String);

/// A manifest's chat template, verified and compiled, ready to render requests.
pub struct ChatRenderer {
    kind: RendererKind,
}

enum RendererKind {
    Chatml,
    Jinja {
        /// Boxed: an environment is large next to the empty `Chatml` variant.
        environment: Box<Environment<'static>>,
        enable_thinking: bool,
    },
}

impl ChatRenderer {
    /// Prepares `template`. For a pinned Jinja template this reads the file, checks its
    /// SHA-256 and compiles it; it fails on a missing file, a hash mismatch, non-UTF-8 text or
    /// a template that does not compile. Blocking (it reads a small file).
    pub fn load(template: &ChatTemplate) -> anyhow::Result<Self> {
        let kind = match template {
            ChatTemplate::Chatml => RendererKind::Chatml,
            ChatTemplate::Jinja(pinned) => load_jinja(pinned)?,
        };
        Ok(Self { kind })
    }

    /// Renders `messages` followed by the opening of the assistant's turn.
    pub fn render(&self, messages: &[ChatMessage]) -> Result<String, RenderError> {
        match &self.kind {
            RendererKind::Chatml => Ok(render_chatml(messages)),
            RendererKind::Jinja {
                environment,
                enable_thinking,
            } => render_jinja(environment, messages, *enable_thinking),
        }
    }
}

fn render_chatml(messages: &[ChatMessage]) -> String {
    let mut prompt: String = messages
        .iter()
        .map(|message| {
            format!(
                "{CHATML_START}{}\n{}{CHATML_END}",
                message.role, message.content
            )
        })
        .collect();
    prompt.push_str(CHATML_ASSISTANT);
    prompt
}

fn load_jinja(pinned: &PinnedTemplate) -> anyhow::Result<RendererKind> {
    let path = &pinned.jinja.path;
    let source = read_pinned(path, &pinned.jinja.sha256)?;
    let environment = jinja_environment(source)
        .with_context(|| format!("chat template {} does not compile", path.display()))?;
    Ok(RendererKind::Jinja {
        environment: Box::new(environment),
        enable_thinking: pinned.enable_thinking,
    })
}

/// Reads the template file and checks it hashes to `expected`.
fn read_pinned(path: &Path, expected: &str) -> anyhow::Result<String> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read chat template {}", path.display()))?;
    let actual = wire::encode_hex(&Sha256::digest(&bytes));
    if !same_sha256(&actual, expected) {
        bail!(
            "chat template {} has SHA-256 {actual}, but chat_template.jinja.sha256 is {expected}",
            path.display()
        );
    }
    String::from_utf8(bytes)
        .with_context(|| format!("chat template {} is not UTF-8", path.display()))
}

/// An environment configured like Hugging Face's chat-template sandbox, holding `source`.
fn jinja_environment(source: String) -> Result<Environment<'static>, Error> {
    let mut environment = Environment::new();
    environment.set_syntax(
        SyntaxConfig::builder()
            .trim_blocks(true)
            .lstrip_blocks(true)
            .build()?,
    );
    environment.set_unknown_method_callback(minijinja_contrib::pycompat::unknown_method_callback);
    environment.add_function(RAISE_EXCEPTION, raise_exception);
    environment.add_template_owned(TEMPLATE_NAME, source)?;
    Ok(environment)
}

/// `raise_exception(msg)`: templates call it to refuse a conversation they cannot format.
fn raise_exception(message: String) -> Result<Value, Error> {
    Err(Error::new(ErrorKind::InvalidOperation, message))
}

fn render_jinja(
    environment: &Environment<'static>,
    messages: &[ChatMessage],
    enable_thinking: bool,
) -> Result<String, RenderError> {
    let messages: Vec<Value> = messages
        .iter()
        .map(|message| {
            context! {
                role => message.role.as_str(),
                content => message.content.as_str(),
            }
        })
        .collect();
    let context = context! {
        messages => messages,
        add_generation_prompt => true,
        enable_thinking => enable_thinking,
        tools => (),
    };
    environment
        .get_template(TEMPLATE_NAME)
        .and_then(|template| template.render(context))
        .map_err(|error| RenderError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::shard::manifest::JinjaFile;

    /// SHA-256 of `tests/fixtures/qwen3-chat-template.jinja`.
    const QWEN3_SHA256: &str = "a55ee1b1660128b7098723e0abcd92caa0788061051c62d51cbe87d9cf1974d8";

    /// david-codex's reference prompt shape (experiments/model-validation/results/
    /// qwen38-three-device-2026-10-24/manifest.json) with the user turn replaced by `<q>`.
    const QWEN3_NO_THINKING: &str = "<|im_start|>system\nYou are a helpful assistant.<|im_end|>\n\
         <|im_start|>user\n<q><|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n";

    fn qwen3_fixture() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("qwen3-chat-template.jinja")
    }

    fn pinned(path: PathBuf, sha256: &str, enable_thinking: bool) -> ChatTemplate {
        ChatTemplate::Jinja(PinnedTemplate {
            jinja: JinjaFile {
                path,
                sha256: sha256.to_owned(),
            },
            enable_thinking,
        })
    }

    fn qwen3(enable_thinking: bool) -> ChatRenderer {
        ChatRenderer::load(&pinned(qwen3_fixture(), QWEN3_SHA256, enable_thinking))
            .expect("load the Qwen3 fixture")
    }

    fn message(role: &str, content: &str) -> ChatMessage {
        ChatMessage {
            role: role.to_owned(),
            content: content.to_owned(),
        }
    }

    fn system_and_user() -> Vec<ChatMessage> {
        vec![
            message("system", "You are a helpful assistant."),
            message("user", "<q>"),
        ]
    }

    /// Writes `source` to a fresh temp file and returns its path and SHA-256.
    fn temp_template(source: &str) -> (PathBuf, String) {
        let path =
            std::env::temp_dir().join(format!("chat-template-{}.jinja", uuid::Uuid::new_v4()));
        std::fs::write(&path, source).unwrap();
        let sha256 = wire::encode_hex(&Sha256::digest(source.as_bytes()));
        (path, sha256)
    }

    #[test]
    fn qwen3_without_thinking_matches_the_ring_demo_prompt_exactly() {
        let prompt = qwen3(false).render(&system_and_user()).unwrap();
        assert_eq!(prompt, QWEN3_NO_THINKING);
    }

    #[test]
    fn qwen3_with_thinking_leaves_out_the_empty_think_block() {
        let prompt = qwen3(true).render(&system_and_user()).unwrap();
        assert_eq!(
            prompt,
            "<|im_start|>system\nYou are a helpful assistant.<|im_end|>\n\
             <|im_start|>user\n<q><|im_end|>\n<|im_start|>assistant\n"
        );
    }

    #[test]
    fn qwen3_strips_reasoning_from_earlier_assistant_turns() {
        // Exercises the pycompat string methods (.split, .rstrip, .lstrip) and namespace logic.
        let messages = vec![
            message("user", "a"),
            message("assistant", "<think>\nhmm\n</think>\n\nb"),
            message("user", "c"),
        ];
        let prompt = qwen3(false).render(&messages).unwrap();
        assert_eq!(
            prompt,
            "<|im_start|>user\na<|im_end|>\n<|im_start|>assistant\nb<|im_end|>\n\
             <|im_start|>user\nc<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
        );
    }

    #[test]
    fn a_hash_mismatch_refuses_to_load() {
        let wrong = "0".repeat(QWEN3_SHA256.len());
        let error = ChatRenderer::load(&pinned(qwen3_fixture(), &wrong, false))
            .err()
            .expect("a mismatched hash must be refused");
        let message = format!("{error:#}");
        assert!(
            message.contains(QWEN3_SHA256) && message.contains(&wrong),
            "{message}"
        );
    }

    #[test]
    fn a_missing_file_refuses_to_load() {
        let missing = std::env::temp_dir().join(format!("missing-{}.jinja", uuid::Uuid::new_v4()));
        let error = ChatRenderer::load(&pinned(missing, QWEN3_SHA256, false))
            .err()
            .expect("a missing template must be refused");
        assert!(format!("{error:#}").contains("failed to read chat template"));
    }

    #[test]
    fn a_template_that_does_not_compile_refuses_to_load() {
        let (path, sha256) = temp_template("{% if messages %}unclosed");
        let error = ChatRenderer::load(&pinned(path.clone(), &sha256, false))
            .err()
            .expect("a broken template must be refused");
        std::fs::remove_file(&path).unwrap();
        assert!(
            format!("{error:#}").contains("does not compile"),
            "{error:#}"
        );
    }

    #[test]
    fn raise_exception_is_a_render_error() {
        let (path, sha256) = temp_template(
            "{% if messages[0].role != 'system' %}\
             {{ raise_exception('a system message is required') }}{% endif %}ok",
        );
        let renderer = ChatRenderer::load(&pinned(path.clone(), &sha256, false)).unwrap();
        std::fs::remove_file(&path).unwrap();
        let error = renderer.render(&[message("user", "hi")]).unwrap_err();
        assert!(
            error.to_string().contains("a system message is required"),
            "{error}"
        );
        assert_eq!(renderer.render(&system_and_user()).unwrap(), "ok");
    }

    #[test]
    fn tools_are_none_and_thinking_is_passed_through() {
        let (path, sha256) = temp_template(
            "{{ tools is none }} {{ enable_thinking }} {{ add_generation_prompt }} \
             {{ messages | length }}",
        );
        let renderer = ChatRenderer::load(&pinned(path.clone(), &sha256, true)).unwrap();
        std::fs::remove_file(&path).unwrap();
        // Booleans print the way Python's Jinja prints them.
        assert_eq!(
            renderer.render(&system_and_user()).unwrap(),
            "True True True 2"
        );
    }

    #[test]
    fn chatml_is_unchanged() {
        let renderer = ChatRenderer::load(&ChatTemplate::Chatml).unwrap();
        assert_eq!(
            renderer.render(&system_and_user()).unwrap(),
            "<|im_start|>system\nYou are a helpful assistant.<|im_end|>\n\
             <|im_start|>user\n<q><|im_end|>\n<|im_start|>assistant\n"
        );
    }
}
