import os
import re
from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import pandas as pd
from difflib import get_close_matches
from werkzeug.utils import secure_filename
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret")

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
ALLOWED_EXT = {"csv"}

def allowed_file(fn):
    return "." in fn and fn.rsplit(".", 1)[1].lower() in ALLOWED_EXT

def sanitize_filename(s):
    # make a safe filename portion
    s = re.sub(r"[^\w\s-]", "", s).strip()
    s = re.sub(r"[-\s]+", "_", s)
    return s.lower()

@app.route("/", methods=["GET", "POST"])
def index():
    # Step 1: collect subject, date, counts
    if request.method == "POST":
        subject = request.form.get("subject", "").strip()
        date_str = request.form.get("date", "").strip()  # expect YYYY-MM-DD or any
        total_count = request.form.get("total_count", "").strip()
        present_count = request.form.get("present_count", "").strip()

        if not subject or not date_str or not total_count or not present_count:
            flash("Please fill all fields.")
            return redirect(url_for("index"))

        # Validate numeric counts
        try:
            total_count = int(total_count)
            present_count = int(present_count)
        except ValueError:
            flash("Counts must be integers.")
            return redirect(url_for("index"))

        # normalize date to YYYY-MM-DD if possible
        try:
            parsed = datetime.fromisoformat(date_str)
            date_norm = parsed.date().isoformat()
        except Exception:
            # try other formats
            try:
                date_norm = datetime.strptime(date_str, "%d-%m-%Y").date().isoformat()
            except Exception:
                # as fallback use raw, but sanitize later for filename
                date_norm = date_str

        # store these on session-like flow: simplest is passing as query params
        return redirect(url_for("upload", subject=subject, date=date_norm,
                                total=total_count, present=present_count))
    return render_template("index.html")

@app.route("/upload", methods=["GET", "POST"])
def upload():
    # read metadata passed via query params
    subject = request.args.get("subject", "")
    date = request.args.get("date", "")
    total = request.args.get("total", "")
    present = request.args.get("present", "")

    if request.method == "POST":
        # get files
        total_file = request.files.get("total_file")
        present_file = request.files.get("present_file")

        if not total_file or not present_file:
            flash("Both CSV files are required.")
            return redirect(request.url)

        if not (allowed_file(total_file.filename) and allowed_file(present_file.filename)):
            flash("Please upload CSV files only.")
            return redirect(request.url)

        # save files
        total_path = os.path.join(UPLOAD_FOLDER, secure_filename("total.csv"))
        present_path = os.path.join(UPLOAD_FOLDER, secure_filename("present.csv"))

        total_file.save(total_path)
        present_file.save(present_path)

        # process
        try:
            total_df = pd.read_csv(total_path, dtype=str)
            present_df = pd.read_csv(present_path, dtype=str)
        except Exception as e:
            flash(f"Error reading CSVs: {e}")
            return redirect(request.url)

        # Standardize expected column name 'StudentName'
        if "StudentName" not in total_df.columns:
            # try first column if unnamed
            total_df.columns = [c if c.strip() else "StudentName" for c in total_df.columns]
            if "StudentName" not in total_df.columns:
                # fallback: take first column as StudentName
                total_df = total_df.rename(columns={total_df.columns[0]: "StudentName"})

        if "StudentName" not in present_df.columns:
            present_df = present_df.rename(columns={present_df.columns[0]: "StudentName"})

        # Clean and normalize names
        total_df["StudentName_clean"] = total_df["StudentName"].astype(str).str.strip().str.lower()
        present_df["StudentName_clean"] = present_df["StudentName"].astype(str).str.strip().str.lower()

        total_names = total_df["StudentName_clean"].tolist()
        present_names_input = present_df["StudentName_clean"].tolist()

        matched_present_indices = set()
        not_found = []  # list of present names not matched to any total student
        matched_pairs = []  # (present_original, matched_total_original, score)

        # For each present name, try to find close match in total_names
        for p_raw in present_df["StudentName"].tolist():  # original string
            p = str(p_raw).strip().lower()
            # exact match quick check
            if p in total_names:
                idx = total_names.index(p)
                matched_present_indices.add(idx)
                matched_pairs.append((p_raw, total_df.at[idx, "StudentName"], "exact"))
            else:
                # difflib close matches
                close = get_close_matches(p, total_names, n=1, cutoff=0.6)
                if close:
                    match = close[0]
                    idx = total_names.index(match)
                    matched_present_indices.add(idx)
                    # find original total name
                    matched_pairs.append((p_raw, total_df.at[idx, "StudentName"], "partial"))
                else:
                    not_found.append(p_raw)

        # Absentees are entries in total_df not matched
        absent_mask = [i not in matched_present_indices for i in range(len(total_df))]
        absentees_df = total_df.loc[absent_mask, :].copy()

        # Prepare output filename
        subj_safe = sanitize_filename(subject or "subject")
        date_safe = sanitize_filename(date or datetime.now().date().isoformat())
        out_name = f"absentees_{subj_safe}_{date_safe}.csv"
        out_path = os.path.join(UPLOAD_FOLDER, secure_filename(out_name))
        absentees_df.drop(columns=["StudentName_clean"], errors="ignore").to_csv(out_path, index=False)

        # pass results to result template
        return render_template("result.html",
                               absentees_count=len(absentees_df),
                               absentees_preview=absentees_df.head(50).to_dict(orient="records"),
                               not_found=not_found,
                               out_filename=out_name,
                               subject=subject,
                               date=date)
    # GET => show upload form
    return render_template("upload.html",
                           subject=subject,
                           date=date,
                           total=total,
                           present=present)

@app.route("/download/<filename>")
def download(filename):
    path = os.path.join(UPLOAD_FOLDER, secure_filename(filename))
    if os.path.exists(path):
        return send_file(path, as_attachment=True, download_name=filename)
    flash("File not found.")
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
