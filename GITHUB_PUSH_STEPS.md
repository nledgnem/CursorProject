# Push to GitHub — Do This Now

Your folder is ready to push. Git is looking for a repo at: **https://github.com/nledgnem/Cursor**

## Step 1: Create the repo on GitHub (if you haven’t)

1. Open: **https://github.com/new**
2. **Repository name:** type exactly: **Cursor**
3. Choose **Public** (or Private if you prefer).
4. **Do not** check "Add a README" or "Add .gitignore" — leave everything unchecked.
5. Click **Create repository**.

## Step 2: Push from your PC

Open **PowerShell** or the terminal in Cursor and run:

```powershell
cd "c:\Users\Admin\Documents\Cursor"
git push -u origin main
```

- If a sign-in window appears, sign in with your GitHub account.
- If it asks for **password**, use a **Personal Access Token** (not your GitHub password):
  - Create one: https://github.com/settings/tokens → **Generate new token (classic)** → tick **repo** → Generate, then copy and paste when asked for password.

---

## If your repo has a different name

If you already created a repo with another name (e.g. `my-project`), run this **instead** of Step 2 (use your real repo name):

```powershell
cd "c:\Users\Admin\Documents\Cursor"
git remote set-url origin https://github.com/nledgnem/YOUR_REPO_NAME.git
git push -u origin main
```

Replace **YOUR_REPO_NAME** with the exact name from your GitHub repo URL.
