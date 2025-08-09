import os
import re
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher, get_close_matches

from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import pandas as pd
from werkzeug.utils import secure_filename

# --- Configuration ---
UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"
ALLOWED_EXT = {"csv"}

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-please-change")


# --- Utilities ---
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT

def sanitize_filename_part(s: str) -> str:
    s = str(s or "")
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s-]+", "_", s.strip())
    return s or "value"

def normalize_name(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if s == "":
        return ""
    # remove accents
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    # remove punctuation except spaces
    s = re.sub(r"[^\w\s]", " ", s)
    # collapse whitespace
    s = " ".join(s.split())
    # remove salutations that commonly appear
    s = re.sub(r"^(mr|ms|mrs|miss|dr|prof|sir)\s+", "", s)
    return s

def choose_name_column(df: pd.DataFrame) -> str:
    if df is None or df.shape[1] == 0:
        raise ValueError("Empty dataframe")
    candidates = {c.lower(): c for c in df.columns}
    for want in ("studentname", "student_name", "name", "full_name", "fullname", "full name"):
        if want in candidates:
            return candidates[want]
    # fallback to first column name
    return df.columns[0]

def pairwise_match(total_df: pd.DataFrame, present_df: pd.DataFrame,
                   name_col_total: str, name_col_present: str,
                   fuzzy_cutoff: float = 0.72, token_jaccard_cutoff: float = 0.5):
    # prepare
    total = total_df.copy().reset_index(drop=True)
    present = present_df.copy().reset_index(drop=True)

    total["__norm"] = total[name_col_total].astype(str).apply(normalize_name)
    present["__norm"] = present[name_col_present].astype(str).apply(normalize_name)

    # maps from normalized name to list of total indices (for fast exact lookups)
    norm_to_indices = {}
    for idx, nm in total["__norm"].items():
        norm_to_indices.setdefault(nm, []).append(idx)

    unmatched_total_indices = set(total.index.tolist())
    matched_pairs = []   # tuples: (present_row_index, total_index, method)
    not_found_present = []  # list of present original strings that couldn't be matched

    total_norm_list = total["__norm"].tolist()
    total_token_sets = [set(n.split()) for n in total_norm_list]

    # helper to allocate a total index when multiple available
    def allocate_from_list(lst):
        for x in lst:
            if x in unmatched_total_indices:
                unmatched_total_indices.remove(x)
                return x
        return None

    for p_idx, p_row in present.iterrows():
        p_orig = present.at[p_idx, name_col_present]
        p_norm = present.at[p_idx, "__norm"]
        if not p_norm:
            not_found_present.append(p_orig)
            continue

        # 1) exact normalized match
        if p_norm in norm_to_indices:
            chosen = allocate_from_list(norm_to_indices[p_norm])
            if chosen is not None:
                matched_pairs.append((p_idx, chosen, "exact"))
                continue

        # 2) token-subset (e.g., "Avesh" in "Avesh Sajiwala")
        p_tokens = set(p_norm.split())
        best_idx = None
        best_score = 0.0
        for t_idx in list(unmatched_total_indices):
            t_tokens = total_token_sets[t_idx]
            if not t_tokens or not p_tokens:
                continue
            inter = p_tokens.intersection(t_tokens)
            # token coverage score: intersection/len(p_tokens)
            coverage = (len(inter) / max(1, len(p_tokens)))
            # also consider intersection relative to union
            jaccard = len(inter) / len(p_tokens.union(t_tokens))
            # prefer high coverage or high jaccard
            score = max(coverage, jaccard)
            if score > best_score:
                best_score = score
                best_idx = t_idx

        if best_idx is not None and best_score >= token_jaccard_cutoff:
            # allocate
            if best_idx in unmatched_total_indices:
                unmatched_total_indices.remove(best_idx)
                matched_pairs.append((p_idx, best_idx, f"token-match:{best_score:.2f}"))
                continue

        # 3) fuzzy matching using SequenceMatcher over normalized strings
        best_idx = None
        best_ratio = 0.0
        for t_idx in list(unmatched_total_indices):
            t_norm = total_norm_list[t_idx]
            if not t_norm:
                continue
            ratio = SequenceMatcher(None, p_norm, t_norm).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_idx = t_idx

        if best_ratio >= fuzzy_cutoff and best_idx is not None:
            unmatched_total_indices.remove(best_idx)
            matched_pairs.append((p_idx, best_idx, f"fuzzy:{best_ratio:.2f}"))
            continue

        # 4) last-chance: try get_close_matches over total_norm_list
        close = get_close_matches(p_norm, [total_norm_list[i] for i in unmatched_total_indices], n=1, cutoff=0.6)
        if close:
            # find index in total_norm_list that equals close[0]
            candidate = None
            for t_idx in list(unmatched_total_indices):
                if total_norm_list[t_idx] == close[0]:
                    candidate = t_idx
                    break
            if candidate is not None:
                unmatched_total_indices.remove(candidate)
                matched_pairs.append((p_idx, candidate, "close-match"))
                continue

        # no match found
        not_found_present.append(p_orig)

    # remaining unmatched_total_indices are absentees
    absentees_idx_sorted = sorted(list(unmatched_total_indices))
    absentees_df = total.loc[absentees_idx_sorted].copy()
    # drop helper column
    absentees_df = absentees_df.drop(columns=["__norm"], errors="ignore")

    return {
        "total_count": len(total),
        "present_records": len(present),
        "matched_count": len(matched_pairs),
        "absentees_count": len(absentees_df),
        "absentees_df": absentees_df,
        "not_found_present": not_found_present,
        "matched_pairs": matched_pairs,
    }


# --- Routes ---
@app.route("/", methods=["GET", "POST"])
def step1():
    # collect subject & date, then redirect to upload page with params
    if request.method == "POST":
        subject = request.form.get("subject", "").strip()
        date = request.form.get("date", "").strip()
        if not subject or not date:
            flash("Please provide both subject and date.")
            return redirect(url_for("step1"))
        # normalize date input for filename; store raw display date separately
        try:
            parsed = datetime.fromisoformat(date)
            date_for_file = parsed.date().isoformat()
            date_display = parsed.date().isoformat()
        except Exception:
            date_for_file = sanitize_filename_part(date)
            date_display = date
        return redirect(url_for("upload", subject=subject, date=date_for_file, date_display=date_display))
    return render_template("index.html")


@app.route("/upload", methods=["GET", "POST"])
def upload():
    subject = request.args.get("subject", "")
    date_for_file = request.args.get("date", "")
    date_display = request.args.get("date_display", date_for_file)

    if request.method == "POST":
        if "total_file" not in request.files or "present_file" not in request.files:
            flash("Both CSV files must be uploaded.")
            return redirect(request.url)
        total_file = request.files["total_file"]
        present_file = request.files["present_file"]
        if total_file.filename == "" or present_file.filename == "":
            flash("Please select both CSV files.")
            return redirect(request.url)
        if not (allowed_file(total_file.filename) and allowed_file(present_file.filename)):
            flash("Only CSV files are supported.")
            return redirect(request.url)

        # save uploads temporarily
        tpath = os.path.join(UPLOAD_DIR, secure_filename("total.csv"))
        ppath = os.path.join(UPLOAD_DIR, secure_filename("present.csv"))
        total_file.save(tpath)
        present_file.save(ppath)

        # read safe with pandas (dtype=str to preserve everything)
        try:
            total_df = pd.read_csv(tpath, dtype=str)
            present_df = pd.read_csv(ppath, dtype=str)
        except Exception as e:
            flash(f"Failed to read CSV: {e}")
            return redirect(request.url)

        # detect name columns
        total_name_col = choose_name_column(total_df)
        present_name_col = choose_name_column(present_df)

        # run matching
        result = pairwise_match(total_df, present_df, total_name_col, present_name_col,
                                fuzzy_cutoff=0.72, token_jaccard_cutoff=0.5)

        # save absentees CSV using original columns
        subj_part = sanitize_filename_part(subject)
        date_part = sanitize_filename_part(date_for_file or date_display or datetime.now().date().isoformat())
        out_basename = f"absentees_{subj_part}_{date_part}.csv"
        out_path = os.path.join(OUTPUT_DIR, secure_filename(out_basename))
        result["absentees_df"].to_csv(out_path, index=False, encoding="utf-8")

        # render result page
        return render_template("result.html",
                               subject=subject,
                               date_display=date_display,
                               total_count=result["total_count"],
                               present_records=result["present_records"],
                               matched_count=result["matched_count"],
                               absentees_count=result["absentees_count"],
                               absentees_preview=result["absentees_df"].head(200).to_dict(orient="records"),
                               not_found_present=result["not_found_present"],
                               out_filename=out_basename)
    # GET: show upload form
    return render_template("upload.html", subject=subject, date_display=date_display)


@app.route("/download/<path:filename>")
def download(filename):
    path = os.path.join(OUTPUT_DIR, secure_filename(filename))
    if not os.path.exists(path):
        flash("File not found.")
        return redirect(url_for("step1"))
    return send_file(path, as_attachment=True, download_name=filename)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
