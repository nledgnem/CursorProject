# Push This Folder to GitHub (First-Time Guide)

You already created a GitHub repository. Follow these steps to push your **entire Cursor folder** to it.

---

## Step 1: Install Git (Required)

Git is not installed on your system. Install it first:

1. **Download Git for Windows**: https://git-scm.com/download/win  
2. Run the installer (default options are fine).  
3. **Close and reopen** Cursor/terminal so it recognizes `git`.

---

## Step 2: Tell Git Who You Are (One-Time)

Open **PowerShell** or **Command Prompt** and run (use your real name and GitHub email):

```powershell
git config --global user.name "Your Name"
git config --global user.email "your-email@example.com"
```

Use the same email as your GitHub account.

---

## Step 3: Go to Your Folder and Initialize Git

```powershell
cd "c:\Users\Admin\Documents\Cursor"
git init
```

---

## Step 4: Add All Files and Commit

```powershell
git add .
git commit -m "Initial commit: push entire Cursor folder"
```

This adds everything in the folder (respecting `.gitignore`, so large data and `venv` are skipped).

---

## Step 5: Connect to Your GitHub Repository

Replace `YOUR_USERNAME` with your GitHub username and `YOUR_REPO_NAME` with the repository name you created:

```powershell
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
```

**Example:** If your repo is `https://github.com/johnsmith/my-project`, then:

```powershell
git remote add origin https://github.com/johnsmith/my-project.git
```

---

## Step 6: Rename Branch to main and Push

```powershell
git branch -M main
git push -u origin main
```

When prompted for **password**, do **not** use your GitHub account password. Use a **Personal Access Token (PAT)**:

1. On GitHub: **Profile (top right) → Settings → Developer settings → Personal access tokens → Tokens (classic)**  
2. **Generate new token (classic)**  
3. Give it a name, choose expiry, check **repo**  
4. Copy the token and paste it when Git asks for a password (nothing will appear as you type—that’s normal).

---

## Summary (Copy-Paste After Installing Git)

After Git is installed and you’ve run the `git config` commands once:

```powershell
cd "c:\Users\Admin\Documents\Cursor"
git init
git add .
git commit -m "Initial commit: push entire Cursor folder"
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git branch -M main
git push -u origin main
```

(Replace `YOUR_USERNAME` and `YOUR_REPO_NAME` with your actual repo URL.)

---

## Before You Push: Security Check

Your **GITHUB_SETUP_GUIDE.md** says to remove API keys from code before pushing. Recommended:

- Open **PRE_COMMIT_CHECKLIST.md** and follow it, **or**
- At least search your code for `api_key`, `API_KEY`, `apikey` and remove or move them to environment variables.

---

## If Something Fails

- **“git is not recognized”** → Install Git (Step 1) and restart the terminal.  
- **“remote origin already exists”** → You already added the remote; use `git push -u origin main` only.  
- **“Authentication failed”** → Use a Personal Access Token as password, not your GitHub password.  
- **“failed to push some refs”** → If the GitHub repo had a README created when you made it, run:  
  `git pull origin main --allow-unrelated-histories`  
  then `git push -u origin main` again.

---

## Easier Option: GitHub Desktop

If you prefer a graphical tool:

1. Install **GitHub Desktop**: https://desktop.github.com/  
2. Sign in with your GitHub account.  
3. **File → Add local repository** → choose `c:\Users\Admin\Documents\Cursor`.  
4. If it says “not a Git repository”, click **create a repository** there.  
5. **Publish repository** and choose your existing GitHub repo if prompted.

You can use either the commands above or GitHub Desktop; both will push your entire Cursor folder to GitHub.
