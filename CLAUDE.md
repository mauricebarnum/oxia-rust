# CLAUDE.md

Refer to the root `AGENTS.md` file for all project architectures, testing frameworks, and style constraints.

## Claude Code-Specific Safety & Workflow Rules
- **Branches**: ALWAYS prefix branches with `claude-` (e.g., `git checkout -b claude-task-name`). Never commit or push directly to `main` or `origin/main`.
- **Isolation**: Write to `./claude-temp/` first (create if needed: `mkdir claude-temp`). Propose diffs with `/diff`; confirm before main files.
- **Git Safety**: 
  - Pre-session: `git stash push -m "pre-claude"`
  - Stash changes during session: `git stash push`
  - Do not commit or push without using `/confirm` or getting explicit user approval.
- **Tokens & Performance**: 
  - Use `/rewind` (Esc Esc) for undos.
  - Limit tools (`ENABLE_TOOL_SEARCH=auto:5%`).
  - Summarize with `/compact`.
  - Monitor `/cost`.
- **Efficiency Prompts**:
  - "Plan in `./claude-temp/`; diff before apply."
  - "Verify with tests; /cost check."

## Claude Code-Specific Overrides
- When compacting history with `/compact`, always preserve the core task list.
- Use subagents for complex research instead of inline exploration loops.