# Communication Rules for Japanese Developers

> **Note**: This file contains communication rules specifically for Japanese-speaking developers. International developers can safely ignore this file.

## Language Requirements

### User Responses (日本語話者向け)
- Use polite and professional language (丁寧語)
- Provide clear explanations and context
- Ask clarifying questions when needed
- Technical explanations should be in Japanese, but code examples remain in English

### Code and Technical Content (All Developers)
- **ALL code MUST be written in English**
- **ALL comments MUST be in English**
- **ALL documentation MUST be in English**
- **ALL error messages MUST be in English**
- **ALL log messages MUST be in English**
- **ALL function names, variable names, struct names MUST be in English**
- **ALL commit messages MUST be in English**

### Examples

#### Correct User Response (Japanese)
```
ファイルの読み込み機能を追加しました。以下のようにCSVファイルを処理できます：

```rust
// Load CSV file with error handling
fn load_csv(path: &str) -> anyhow::Result<DataFrame> {
    let df = LazyFrame::scan_csv(path, ScanArgsCSV::default())?
        .collect()?;
    Ok(df)
}
```

この実装では、エラーハンドリングを適切に行い、大きなファイルでも効率的に処理できます。
```

## Rule Management Protocol (日本語話者向け)

### Automatic Rule Addition
Add rules to `.cursor/rules` automatically when:
- User provides clear, universally beneficial development guidelines
- Technical standards that improve code quality
- Project-specific conventions that are consistently applicable

### User Confirmation Required
Ask for user confirmation before adding rules when:
- Rules might significantly change development workflow
- Personal preferences that might not apply to all developers
- Complex or controversial guidelines
- Rules that might conflict with existing standards

### Rule Addition Process
- Identify → Determine auto-add vs confirmation → Add to `.cursor/rules/` → Inform user
- Confirmation phrase: "この内容を.cursor/rulesに追加してもよろしいでしょうか？"

## Communication Style (日本語話者向け)

### Technical Explanations
- Provide both high-level overview and detailed implementation
- Include code examples when helpful
- Explain the reasoning behind technical decisions
- Reference relevant documentation or standards

### Error Reporting
- Clearly describe what went wrong
- Provide steps to reproduce if applicable
- Suggest potential solutions
- Include relevant file paths and line numbers

### Progress Updates
- Keep users informed of long-running operations
- Explain what is being done and why
- Provide estimated completion times when possible

## Code Review and Feedback

### When Reviewing Code
- Focus on functionality, readability, and performance
- Suggest improvements with explanations
- Provide examples of better approaches
- Consider maintainability and scalability

### When Providing Feedback
- Be constructive and specific
- Explain the benefits of suggested changes
- Acknowledge good practices when present
- Prioritize feedback by importance 
