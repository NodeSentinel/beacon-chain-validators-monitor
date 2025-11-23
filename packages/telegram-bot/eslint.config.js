import antfu from '@antfu/eslint-config';

export default antfu(
  {},
  {
    files: ['src/**/*.{ts,js}'],
    languageOptions: {
      globals: {
        // Allow Node.js globals in this package
        process: 'readonly',
        __dirname: 'readonly',
        module: 'readonly',
        require: 'readonly',
      },
    },
    rules: {
      // Allow using global `process` in Node context
      'node/prefer-global/process': 'off',
    },
  },
);
