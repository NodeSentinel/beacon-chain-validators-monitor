import { PRIMARY_OPTION_ICON, SECONDARY_OPTION_ICON } from './options.js';

interface CommunicationPreview {
  id: number;
  description: string;
  options: string[];
}

/**
 * Escapes text used inside Telegram HTML metadata messages.
 */
function escapeHtml(text: string) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Formats the metadata shown before a communication preview.
 */
export function formatCommunicationPreviewHeader(communication: CommunicationPreview) {
  const lines = [
    `<b>Communication #${communication.id}</b>`,
    `Description: ${escapeHtml(communication.description)}`,
  ];

  // Show the buttons the broadcast will carry so the admin can check them
  // before sending, since the preview message itself has no keyboard.
  if (communication.options.length > 0) {
    lines.push(`Buttons: ${formatOptionLabels(communication.options)}`);
  }

  lines.push(`Send: /send_communication ${communication.id}`);

  return lines.join('\n');
}

/**
 * Renders the option labels with the icon each one gets in the keyboard.
 */
export function formatOptionLabels(options: string[]) {
  return options
    .map(
      (label, optionIndex) =>
        `${optionIndex === 0 ? PRIMARY_OPTION_ICON : SECONDARY_OPTION_ICON} ${escapeHtml(label)}`,
    )
    .join(' / ');
}

/**
 * Formats the answer tally of one communication that shipped with buttons.
 */
export function formatCommunicationResults(communication: {
  id: number;
  description: string;
  options: string[];
  responses: Array<{ optionIndex: number; label: string; count: number }>;
}) {
  if (communication.options.length === 0) {
    return `Communication #${communication.id} was sent without buttons.`;
  }

  const totalAnswers = communication.responses.reduce(
    (total, response) => total + response.count,
    0,
  );

  return [
    `<b>Communication #${communication.id}</b>`,
    `Description: ${escapeHtml(communication.description)}`,
    ...communication.responses.map(
      (response) =>
        `${response.optionIndex === 0 ? PRIMARY_OPTION_ICON : SECONDARY_OPTION_ICON} ${escapeHtml(
          response.label,
        )}: ${response.count}`,
    ),
    `Total answers: ${totalAnswers}`,
  ].join('\n');
}
