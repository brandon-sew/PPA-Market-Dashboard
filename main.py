name: Daily Market Update
on:
  schedule:
    - cron: '0 13 * * *' # Runs at 13:00 UTC (14:00 CET)
  workflow_dispatch: 

# This block is the new "Secret Sauce" that grants write access
permissions:
  contents: write

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4 # Updated version
      - name: Set up Python
        uses: actions/setup-python@v5 # Updated version
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run extraction
        env:
          ENTSOE_TOKEN: ${{ secrets.ENTSOE_TOKEN }}
        run: python main.py
      - name: Commit and push changes
        run: |
          git config --global user.name 'github-actions[bot]'
          git config --global user.email 'github-actions[bot]@users.noreply.github.com'
          git add market_prices.csv
          git commit -m "Update market prices" || echo "No changes to commit"
          git push
