# Quarto Helpers
This contains some useful tools and templates designed for working with Quarto and RMarkdown sites. However a lot of these tools can be easily modified to help in the development of other static sites.

## Tools
### optimize_quarto.R
This script should be run post-render. It will go through all of the files in the _site folder and minify them where possible. This will reduce the file sizeand increase page-load performance. It can be added to any quarto website project by adding the below to your _quarto.yml file. Quarto comes with Deno as it's TS/JS runtime, so nothing else needs installed.
```
project:
  type: website
  post-render: optimize_quarto.ts
```
