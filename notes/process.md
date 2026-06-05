# Overview

This document will be used to record notes from how I'm going to add a metrics
implementation mostly using AI chat bots and agents along with some of my own
research. Let's see how it goes.

## Objective

I want to collect useful metrics with minimal overhead and without being to
opinionated on metrics back-ends or other integration points.

## History

### Initial research

Selected the
[opentelemetry](https://docs.rs/opentelemetry/latest/opentelemetry/) crate for
the metrics API. This seems to be increasingly adopted as a lightweight metrics
facade with a richer model than what is provided by
[metrics](https://crates.io/crates/metrics).

### Design prompt

- First pass generated with perplexity.ai
  - [Prompt](./otel_design_prompt_v1.md)
  - [Session](https://www.perplexity.ai/search/bfe2dd98-c08a-45c2-9f03-90c1cd15dcc1?preview=1)

- Reviewed by claude and asked it to apply suggestions
  - [Prompt](./otel_design_prompt_v2.md)
  - [Session](https://claude.ai/share/177c60c9-1b42-4b32-ac04-308fb74be5c0)

- Reviewed by chatgpt and asked it to apply suggestions
  - [Prompt](./otel_design_prompt_v3.md)
  - [Session](https://chatgpt.com/share/6a17114f-0194-83e8-ad2a-c04215390b5b)
    
- Reviewed by gemini and asked it to apply suggestions
  - [Prompt](./otel_design_prompt_v4.md)
  - [Session](https://gemini.google.com/share/bb56f9daf3b8)

- Edited the prompt, making only minor tweaks
  - [Prompt](./otel_design_prompt_pixi.md)
  
### Agent design

- First asked Antigravity cli to read plan and generate a design doc
    - [Session](./agy-design.md)
- See commit history of [./otel-instrumentation-design.md][] for further iterations

### Implementation
- Attempt to use OpenCode with qwen3-coder:30b via Ollama was a failure:
generation took hours and the result was incomplete.  See branch
`telemetry-opencode-ollama-one-promopt` for that experiment
- Next used codex, see partial session transcripts in [../.specstory/history][], but output
    of `otel-instrumentation-design.md` has since been moved to this directory
  
