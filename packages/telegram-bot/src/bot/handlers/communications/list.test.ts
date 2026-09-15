import assert from 'node:assert/strict';
import test from 'node:test';

import { formatCommunicationPreviewHeader, formatCommunicationResults } from './list-format.js';

test('formatCommunicationPreviewHeader shows id description and send command', () => {
  // This communication represents one pending broadcast returned by the API.
  const communication = {
    id: 7,
    description: 'Maintenance window',
    options: [],
  };

  // This call builds the metadata message shown before the real preview body.
  const header = formatCommunicationPreviewHeader(communication);

  // This assertion verifies admins can identify and send the communication from the list.
  assert.equal(
    header,
    '<b>Communication #7</b>\nDescription: Maintenance window\nSend: /send_communication 7',
  );
});

test('formatCommunicationPreviewHeader escapes html in the communication description', () => {
  // This communication has characters that would be parsed as Telegram HTML.
  const communication = {
    id: 8,
    description: 'Alert <mainnet> & validators',
    options: [],
  };

  // This call protects metadata while leaving the real preview body to be sent separately.
  const header = formatCommunicationPreviewHeader(communication);

  // This assertion verifies the metadata message remains valid HTML.
  assert.equal(
    header,
    '<b>Communication #8</b>\nDescription: Alert &lt;mainnet&gt; &amp; validators\nSend: /send_communication 8',
  );
});

// A communication created with buttons must expose them in the pending preview,
// because the preview message itself is sent without a keyboard.
test('formatCommunicationPreviewHeader lists the configured buttons', () => {
  // This communication ships two options in semantic order: accept first.
  const communication = {
    id: 9,
    description: 'Cluster migration',
    options: ['Aceptar', 'Cancelar'],
  };

  // This call builds the metadata message admins read before sending.
  const header = formatCommunicationPreviewHeader(communication);

  // This assertion verifies each label is shown with the icon it will carry,
  // primary first, matching the stored order rather than the rendered row.
  assert.equal(
    header,
    [
      '<b>Communication #9</b>',
      'Description: Cluster migration',
      'Buttons: \u2705 Aceptar / \u274c Cancelar',
      'Send: /send_communication 9',
    ].join('\n'),
  );
});

// After a broadcast, admins need the tally of what users answered.
test('formatCommunicationResults counts the answers of every option', () => {
  // This communication was answered by 52 users: 40 accepted, 12 cancelled.
  const communication = {
    id: 9,
    description: 'Cluster migration',
    options: ['Aceptar', 'Cancelar'],
    responses: [
      { optionIndex: 0, label: 'Aceptar', count: 40 },
      { optionIndex: 1, label: 'Cancelar', count: 12 },
    ],
  };

  // This call builds the admin-facing summary of the stored answers.
  const results = formatCommunicationResults(communication);

  // This assertion verifies each option is reported with its own count plus the total.
  assert.equal(
    results,
    [
      '<b>Communication #9</b>',
      'Description: Cluster migration',
      '\u2705 Aceptar: 40',
      '\u274c Cancelar: 12',
      'Total answers: 52',
    ].join('\n'),
  );
});

// Plain text communications have nothing to tally, and the command must say so
// instead of reporting an empty result that looks like zero answers.
test('formatCommunicationResults explains communications sent without buttons', () => {
  // This communication was created with the plain text form of the command.
  const communication = {
    id: 3,
    description: 'Maintenance window',
    options: [],
    responses: [],
  };

  // This call handles the case where the admin asks for results of a plain broadcast.
  const results = formatCommunicationResults(communication);

  // This assertion verifies the admin gets an explicit reason, not an empty tally.
  assert.equal(results, 'Communication #3 was sent without buttons.');
});

// Options without answers are absent from the API response, so the formatter
// must show only actual answers instead of inventing zero-count result rows.
test('formatCommunicationResults omits options without answers', () => {
  // This communication offered two options, but all 12 respondents selected cancel.
  const communication = {
    id: 9,
    description: 'Cluster migration',
    options: ['Aceptar', 'Cancelar'],
    responses: [{ optionIndex: 1, label: 'Cancelar', count: 12 }],
  };

  // This call formats the sparse response collection returned by the API.
  const results = formatCommunicationResults(communication);

  // This assertion verifies the unanswered accept option is not rendered.
  assert.equal(
    results,
    [
      '<b>Communication #9</b>',
      'Description: Cluster migration',
      '\u274c Cancelar: 12',
      'Total answers: 12',
    ].join('\n'),
  );
});
