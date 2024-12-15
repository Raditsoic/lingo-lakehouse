/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        eel: '#4b4b4b',
        swan: '#e5e5e5',
        snow: '#ffffff',
        featherGreen: '#58cc02',
        maskGreen: '#89e219'
      },
    },
  },
  plugins: [],
}

