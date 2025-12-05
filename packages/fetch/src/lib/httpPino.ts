import { AxiosError, AxiosResponse, InternalAxiosRequestConfig } from 'axios';

// NOTE: Using process.env directly instead of importing env.ts to avoid triggering
// full environment variable validation when this module is loaded.
// This is an infrastructure/logging module that should work independently of
// blockchain-specific environment variables.
import createLogger from '@/src/lib/pino.js';

// Create the HTTP logger using the existing createLogger function with blue color
const httpLogger = createLogger('HTTP', true, 'blue');

/**
 * Extract endpoint path from a full URL
 * Returns just the path and query string, without the base URL
 */
function extractEndpointPath(url: string | undefined): string {
  if (!url) return 'unknown';
  try {
    const urlObj = new URL(url);
    return urlObj.pathname + urlObj.search;
  } catch {
    // If URL parsing fails, try to extract path manually
    const match = url.match(/https?:\/\/[^/]+(\/.*)/);
    return match ? match[1] : url;
  }
}

export function logRequest(request: InternalAxiosRequestConfig): InternalAxiosRequestConfig {
  // Log the request only if LOG_LEVEL is debug
  const isDebugLevel = process.env.LOG_LEVEL === 'debug';

  if (isDebugLevel) {
    const endpoint = extractEndpointPath(request.url);
    httpLogger.info(`${request.method?.toUpperCase()} ${endpoint}`);
  }

  return request;
}

export function logResponse(response: AxiosResponse): AxiosResponse {
  // Log the response using the custom logger format
  const isError = response.status >= 400;
  const isDebugLevel = process.env.LOG_LEVEL === 'debug';
  const endpoint = extractEndpointPath(response.config?.url);
  const message = `${response.status} ${response.config?.method?.toUpperCase()} ${endpoint}`;

  // Always log errors, or log all responses if LOG_LEVEL is debug
  if (isError || isDebugLevel) {
    if (isError) {
      httpLogger.error(message, { statusCode: response.status });
    } else {
      httpLogger.info(message);
    }
  }

  return response;
}

export function logError(error: AxiosError): Promise<never> {
  // Log the error with endpoint information
  const endpoint = extractEndpointPath(error.config?.url || error.request?.url);
  const statusCode = error.response?.status;
  const method = error.config?.method?.toUpperCase() || 'UNKNOWN';
  const message = statusCode
    ? `${statusCode} ${method} ${endpoint}`
    : `ERROR ${method} ${endpoint}`;

  httpLogger.error(message, {
    statusCode,
    error: error.message,
  });

  return Promise.reject(error);
}
