export interface ParsedCreateCommunicationCommand {
  description: string;
  message: string;
  options: string[];
}

const CREATE_COMMUNICATION_SEPARATOR = '>>';
const CREATE_COMMUNICATION_OPTIONS_SEPARATOR = '|';
const CREATE_COMMUNICATION_OPTIONS_COUNT = 2;

/**
 * Reads the optional trailing options block of the command.
 * A block only counts as options when it holds exactly two non-empty labels
 * split by a pipe, so a message that happens to contain the separators is still
 * treated as plain text.
 */
function parseOptionsBlock(text: string): string[] | null {
  const labels = text.split(CREATE_COMMUNICATION_OPTIONS_SEPARATOR).map((label) => label.trim());

  if (labels.length !== CREATE_COMMUNICATION_OPTIONS_COUNT) return null;
  if (labels.some((label) => label.length === 0)) return null;

  return labels;
}

/**
 * Parses the communication fields from the /create_communication command text.
 * The first separator splits description from message. A second separator is
 * optional and, when its trailing block looks like an options list, turns the
 * communication into one that ships with buttons. Options are kept in the order
 * the admin wrote them: primary action first.
 */
export function parseCreateCommunicationCommand(
  text: string | undefined,
): ParsedCreateCommunicationCommand | null {
  if (!text) return null;

  const commandBody = text.replace(/^\/create_communication(?:@\S+)?\s*/u, '');
  const separatorIndex = commandBody.indexOf(CREATE_COMMUNICATION_SEPARATOR);

  if (separatorIndex === -1) return null;

  const description = commandBody.slice(0, separatorIndex).trim();
  const messageBody = commandBody.slice(separatorIndex + CREATE_COMMUNICATION_SEPARATOR.length);

  // Look at the last separator so messages containing ">>" keep working.
  const optionsSeparatorIndex = messageBody.lastIndexOf(CREATE_COMMUNICATION_SEPARATOR);
  const options =
    optionsSeparatorIndex === -1
      ? null
      : parseOptionsBlock(
          messageBody.slice(optionsSeparatorIndex + CREATE_COMMUNICATION_SEPARATOR.length),
        );

  const message = (options ? messageBody.slice(0, optionsSeparatorIndex) : messageBody).trim();

  if (!description || !message) return null;

  return {
    description,
    message,
    options: options ?? [],
  };
}
