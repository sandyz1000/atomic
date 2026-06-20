/**
 * Distributed subagent example using JsRdd.agentStep (TypeScript binding).
 *
 * Runs a framework-native LLM agent loop over each partition of a document set.
 * Each partition is processed by a subagent that extracts key obligations from
 * a legal clause, returning one finding object per input string.
 *
 * Setup:
 *   cd examples/ts-demo && npm install
 *   npm run build:native
 *
 *   export OPENAI_API_KEY=sk-...     # OpenAI (default provider)
 *   export ANTHROPIC_API_KEY=sk-...  # or Anthropic Claude
 *
 * If neither key is set, the example exits with a clear message.
 *
 * Run:
 *   npm run agentstep
 */

import Atomic = require('@atomic-compute/js');

const provider = process.env['OPENAI_API_KEY']
  ? 'openai'
  : process.env['ANTHROPIC_API_KEY']
    ? 'anthropic'
    : null;

if (provider === null) {
  console.log('agent_step example: skipped -- set OPENAI_API_KEY or ANTHROPIC_API_KEY to run.');
  process.exit(0);
}

const model = provider === 'openai' ? 'gpt-4o-mini' : 'claude-haiku-4-5-20251001';

const ctx = new Atomic.Context();

const docs: string[] = [
  'The Supplier shall deliver all goods within 30 days of purchase order receipt.',
  'Late deliveries incur a penalty of 2% per week, capped at 10% of contract value.',
  'Either party may terminate this agreement with 90 days written notice.',
  'The Buyer warrants payment within 45 days of invoice date.',
];

console.log(`Running agentStep over ${docs.length} documents (${provider}/${model})...`);

const findings = ctx.parallelize(docs, 2).agentStep({
  model,
  systemPrompt: 'Extract the key obligation (who must do what by when) as one sentence.',
  maxRounds: 2,
  provider,
  maxTokensTotal: 20_000,
});

for (const f of findings) {
  console.log(`[input ${f.inputId}] rounds=${f.rounds} budgetExceeded=${f.budgetExceeded}`);
  console.log(`  -> ${f.answer}\n`);
}

if (findings.length !== docs.length) {
  throw new Error('expected one finding per input document');
}
console.log(`agentStep example completed successfully (${findings.length} findings).`);
