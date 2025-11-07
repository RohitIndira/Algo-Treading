# Git Setup Instructions

Follow these commands to initialize git, create dev branch, and push to GitHub:

## Step 1: Stage and Commit All Files

```bash
cd /home/rohitt/Desktop/trading-system

# Add all files
git add .

# Commit with initial message
git commit -m "Initial commit: Complete trading system architecture and structure"
```

## Step 2: Create Dev Branch

```bash
# Create and switch to dev branch
git checkout -b dev
```

## Step 3: Add Remote and Push

```bash
# Add GitHub remote
git remote add origin https://github.com/RohitIndira/Algo-Treading.git

# Push dev branch to GitHub
git push -u origin dev

# Optionally push master branch too
git checkout master
git push -u origin master

# Switch back to dev for development
git checkout dev
```

