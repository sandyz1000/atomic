/**
 * Distributed subagent with an inline **JS tool** using JsRdd.agentStep.
 *
 * This is the *scripted* tool lane: the tool is an ordinary JS function shipped
 * with the job as source — no binary rebuild, no redeploy. The worker's embedded
 * V8 (deno_core) runtime executes it when the subagent emits
 * `TOOL_CALL: <name> <json_args>`. (Contrast the Rust `examples/agent_code_audit`,
 * where tools are compiled `#[task]` functions baked into the binary.)
 *
 * A tool is just an ordinary task in an LLM-callable role: write a function
 * `(args) => result`, give it a name, and pass it under `tools`. The binding ships
 * its source in `AgentStepPayload.resolvedTools`; changing the tool never requires
 * a recompile.
 *
 * Setup:
 *   cd examples/ts-demo && npm install
 *   npm run build:native
 *
 *   export OPENAI_API_KEY=sk-...     # OpenAI (default provider)
 *   export ANTHROPIC_API_KEY=sk-...  # or Anthropic Claude
 *
 * Run:
 *   npm run agentstep:tool
 */

import Atomic = require('@atomic-compute/js');

const provider = process.env['OPENAI_API_KEY']
  ? 'openai'
  : process.env['ANTHROPIC_API_KEY']
    ? 'anthropic'
    : null;

if (provider === null) {
  console.log('agent_step_tool example: skipped -- set OPENAI_API_KEY or ANTHROPIC_API_KEY to run.');
  process.exit(0);
}

const model = provider === 'openai' ? 'gpt-4o-mini' : 'claude-haiku-4-5-20251001';

const ctx = new Atomic.Context();

// A JS tool: maps a severity label to a numeric weight. Shipped as source — the
// worker invokes it as `(args) => result` with the JSON args the model supplies.
const severityWeightTool = {
  name: 'severity_weight',
  source: `(args) => {
    const weights = { CRITICAL: 4, HIGH: 3, MEDIUM: 2, LOW: 1 };
    return { weight: weights[args.label] || 0 };
  }`,
};

const issues: string[] = [
  'Unbounded user input is concatenated directly into a SQL query string.',
  'A config value is read once at startup and cached for the process lifetime.',
  'Two threads write to a shared file without a lock.',
  'A debug log line prints a full request body including an auth header.',
];

console.log(`Triaging ${issues.length} issues with an inline JS tool (${provider}/${model})...`);

const findings = ctx.parallelize(issues, 2).agentStep({
  model,
  systemPrompt:
    'You are a security triage agent. Classify the issue\'s severity as one of ' +
    'CRITICAL/HIGH/MEDIUM/LOW. Call the `severity_weight` tool with ' +
    '{"label": "<your label>"} to fetch its numeric weight, then respond with:\n' +
    'FINAL ANSWER: <LABEL> (weight <N>) — <one-sentence justification>',
  maxRounds: 4,
  provider,
  tools: [severityWeightTool],
  maxTokensTotal: 20_000,
});

for (const f of findings) {
  console.log(`[input ${f.inputId}] rounds=${f.rounds} budgetExceeded=${f.budgetExceeded}`);
  console.log(`  -> ${f.answer}\n`);
}

if (findings.length !== issues.length) {
  throw new Error('expected one finding per input issue');
}
console.log(`agent_step_tool example completed successfully (${findings.length} findings).`);
