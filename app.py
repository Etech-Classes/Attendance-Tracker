import os
from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import pandas as pd
from werkzeug.utils import secure_filename
import tempfile

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "change-this-for-prod")

# Use a writable ephemeral directory (Render's filesystem is ephemeral)
UPLOAD_FOLDER = "/tmp/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
ALLOWED_EXTENSIONS = {"csv"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/process", methods=["POST"])
def process():
    if "total_file" not in request.files or "present_file" not in request.files:
        flash("Both files are required.")
        return redirect(url_for("index"))

    total_file = request.files["total_file"]
    present_file = request.files["present_file"]

    if total_file.filename == "" or present_file.filename == "":
        flash("Please select both CSV files.")
        return redirect(url_for("index"))

    if not (allowed_file(total_file.filename) and allowed_file(present_file.filename)):
        flash("Only CSV files are allowed.")
        return redirect(url_for("index"))

    total_path = os.path.join(UPLOAD_FOLDER, secure_filename("total.csv"))
    present_path = os.path.join(UPLOAD_FOLDER, secure_filename("present.csv"))

    total_file.save(total_path)
    present_file.save(present_path)

    # Load CSVs safely
    try:
        total_df = pd.read_csv(total_path)
        present_df = pd.read_csv(present_path)
    except Exception as e:
        flash(f"Error reading CSV: {e}")
        return redirect(url_for("index"))

    # Ensure required column exists
    if "StudentName" not in total_df.columns:
        flash("Total students CSV must contain column 'StudentName' (and optional 'Batch').")
        return redirect(url_for("index"))
    if "StudentName" not in present_df.columns:
        flash("Present students CSV must contain column 'StudentName'.")
        return redirect(url_for("index"))

    # Normalize and compute absentees
    total_df["StudentName"] = total_df["StudentName"].astype(str).str.strip().str.lower()
    present_df["StudentName"] = present_df["StudentName"].astype(str).str.strip().str.lower()

    absentees_df = total_df[~total_df["StudentName"].isin(present_df["StudentName"])]

    # Save to temp file
    out_path = os.path.join(UPLOAD_FOLDER, "absentees.csv")
    absentees_df.to_csv(out_path, index=False)

    return render_template("index.html", absentees=True)

@app.route("/download")
def download():
    out_path = os.path.join(UPLOAD_FOLDER, "absentees.csv")
    if not os.path.exists(out_path):
        flash("No absentees file found. Please upload & process first.")
        return redirect(url_for("index"))
    return send_file(out_path, as_attachment=True, download_name="absentees.csv")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
