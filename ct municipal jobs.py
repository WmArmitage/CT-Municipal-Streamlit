import streamlit as st
import pandas as pd
import json
import csv
import urllib.request

# Page configuration
st.set_page_config(
    page_title="Connecticut Municipal Employment Directory",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATASET_PURCHASE_URL = "https://ko-fi.com/s/814c806c0b"
SUPPORT_URL = "https://ko-fi.com/wmarmitage"
SUPPORT_MESSAGE = "This directory is free to use. If it saves you time, you can support the project."
SUPPORT_BUTTON_LABEL = "Support the project"
TOP_SUPPORTING_LINE = "All Connecticut municipal job links in one place."
TOP_BODY_LINE = (
    "Skip checking 169 municipal websites. Use the free directory to find official job pages "
    "and application links, or download the full dataset for professional use."
)
TOP_MUTED_LINE = "Built for job seekers, recruiting, outreach, research, and vendor prospecting."
STATS_LINE_ONE = "Covers all 169 Connecticut municipalities."
STATS_LINE_TWO = "This replaces checking 169 separate municipal websites manually."
BROWSE_BODY_LINE = "Search by town or platform and go directly to official job pages and application forms."
BOTTOM_DATASET_HEADER = "Get the full dataset in one file"

FILTER_DEFAULTS = {
    "search_term": "",
    "selected_platform": "All Platforms",
    "show_job_page": True,
    "show_no_job_page": True,
    "show_app_form": True,
    "show_no_app_form": True,
    "verified_links_only": False,
    "hide_suspicious_links": False,
    "filter_verified_employment": False,
    "filter_application_pdf": False,
    "filter_third_party_platform": False,
    "filter_manual_pdf_process": False,
}

# Hide Streamlit chrome
st.markdown("""
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
#GithubIcon {visibility: hidden !important; display: none !important;}
#ForkLink {visibility: hidden !important; display: none !important;}

/* Fallbacks for Streamlit header action variants */
[data-testid="stToolbarActionButton"] {
    display: none !important;
    visibility: hidden !important;
}

[data-testid="stToolbarActionButtonIcon"] {
    display: none !important;
    visibility: hidden !important;
}

header [data-testid="stHeaderActionElements"] a[href*="github.com"],
header [data-testid="stHeaderActionElements"] [title*="GitHub"],
header [data-testid="stHeaderActionElements"] [aria-label*="GitHub"],
header [data-testid="stHeaderActionElements"] [title*="Fork"],
header [data-testid="stHeaderActionElements"] [aria-label*="Fork"] {
    visibility: hidden !important;
    display: none !important;
}

/* Hide top toolbar actions such as Fork/GitHub links when present */
header [data-testid="stToolbar"] a[href*="github.com"],
header [data-testid="stToolbar"] a[title*="GitHub"],
header a[href*="github.com"],
header a[title*="GitHub"],
header button[title*="Fork"],
header [aria-label*="Fork"] {
    display: none !important;
}

/* Keep sidebar collapsed-control visible above any header layer */
div[data-testid="collapsedControl"] {
    z-index: 1001 !important;
}
</style>
""", unsafe_allow_html=True)

st.markdown(
    """
    <style>
    /* Main content area */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2.5rem;
    }

    /* Keep sidebar readable and clearly separate */
    section[data-testid="stSidebar"] {
        background-color: #F1F5F9;
        border-right: 1px solid #E2E8F0;
    }

    section[data-testid="stSidebar"] .block-container {
        padding-top: 1.25rem;
    }

    /* Typography */
    h1, h2, h3 {
        letter-spacing: -0.02em;
        color: #0F172A;
    }

    h1 {
        margin-bottom: 0.35rem;
    }

    h2 {
        margin-top: 0.75rem;
        margin-bottom: 0.5rem;
    }

    p, li, label, .stMarkdown, .stCaption {
        color: #334155;
    }

    /* Caption readability */
    .stCaption {
        opacity: 0.95;
    }

    /* Buttons */
    div.stLinkButton > a {
        border-radius: 12px !important;
        padding: 0.85rem 1.25rem !important;
        font-weight: 700 !important;
        text-decoration: none !important;
        color: #FFFFFF !important;
        background-color: #EA580C !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        transition: all 0.15s ease-in-out;
    }

    div.stLinkButton > a:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 18px rgba(0,0,0,0.2);
        background-color: #C2410C !important;
        color: #FFFFFF !important;
    }

    div.stLinkButton > a:visited {
        color: #FFFFFF !important;
    }

    div.stLinkButton > a *,
    div.stLinkButton [data-testid="stMarkdownContainer"],
    div.stLinkButton [data-testid="stMarkdownContainer"] p {
        color: #FFFFFF !important;
    }

    /* Secondary support CTA (blue, quieter than primary dataset CTA) */
    div.stLinkButton > a[href*="ko-fi.com/wmarmitage"],
    a.secondary-support-button {
        display: inline-block;
        padding: 0.58rem 0.9rem !important;
        border-radius: 10px;
        font-weight: 600 !important;
        text-decoration: none !important;
        color: #1D4ED8 !important;
        background-color: #EFF6FF !important;
        border: 1px solid #BFDBFE !important;
        box-shadow: 0 1px 3px rgba(29, 78, 216, 0.12) !important;
        transition: all 0.15s ease-in-out !important;
    }

    div.stLinkButton > a[href*="ko-fi.com/wmarmitage"]:hover,
    a.secondary-support-button:hover {
        background-color: #DBEAFE !important;
        border-color: #93C5FD !important;
        color: #1E3A8A !important;
        box-shadow: 0 2px 5px rgba(29, 78, 216, 0.16) !important;
        transform: none !important;
    }

    div.stLinkButton > a[href*="ko-fi.com/wmarmitage"]:visited,
    a.secondary-support-button:visited {
        color: #1D4ED8 !important;
    }

    div.stLinkButton > a[href*="ko-fi.com/wmarmitage"] *,
    div.stLinkButton > a[href*="ko-fi.com/wmarmitage"] [data-testid="stMarkdownContainer"],
    div.stLinkButton > a[href*="ko-fi.com/wmarmitage"] [data-testid="stMarkdownContainer"] p {
        color: #1E3A8A !important;
    }

    /* Right-side product card */
    div[data-testid="stHorizontalBlock"]:first-of-type > div[data-testid="column"]:last-child > div[data-testid="stVerticalBlock"] {
        background: #FFFFFF;
        border: 1px solid #E5E7EB !important;
        border-radius: 12px !important;
        box-shadow: 0 6px 18px rgba(0,0,0,0.08);
        padding: 1.5rem;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        padding: 1rem 1rem 0.8rem 1rem;
        border-radius: 14px;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
    }

    /* Alerts/info/warning boxes */
    [data-testid="stAlert"] {
        border-radius: 12px;
    }

    div[data-baseweb="notification"][kind="warning"] {
        background-color: #FEF3C7;
        border: 1px solid #FDE68A;
        border-left: 4px solid #F59E0B;
    }

    div[data-baseweb="notification"][kind="info"] {
        background-color: #EFF6FF;
        border: 1px solid #DBEAFE;
        border-left: 4px solid #60A5FA;
    }

    /* Horizontal rules */
    hr {
        margin-top: 2rem !important;
        margin-bottom: 2rem !important;
        border-color: #E2E8F0 !important;
    }

    /* Dataframe/table container */
    [data-testid="stDataFrame"], .stTable {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid #E2E8F0;
    }

    /* Inputs and selects feel cleaner */
    input, textarea, [data-baseweb="select"] {
        border-radius: 10px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Load data
@st.cache_data
def load_employment_data():
    """Load the CT municipal employment data from GitHub"""
    url = "https://raw.githubusercontent.com/WmArmitage/CT-Municipal-Streamlit/refs/heads/main/CT_Municipal_Employment_Pages.json"
    
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Unable to load employment data from GitHub: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def load_link_health_lookup():
    """Load link-health metadata from GitHub report CSV when available."""
    sources = [
        "https://raw.githubusercontent.com/WmArmitage/CT-Municipal-Streamlit/refs/heads/main/reports/link_health_report.csv",
        "reports/link_health_report.csv",
    ]
    lookup = {}
    for source in sources:
        try:
            if source.startswith("http"):
                with urllib.request.urlopen(source) as response:
                    content = response.read().decode("utf-8")
            else:
                with open(source, "r", encoding="utf-8", newline="") as handle:
                    content = handle.read()

            reader = csv.DictReader(content.splitlines())
            for row in reader:
                town = str(row.get("Town") or "").strip().lower()
                field_name = str(row.get("field_name") or "").strip().lower()
                if not field_name:
                    field_alias = str(row.get("Field") or "").strip().lower()
                    if "employment" in field_alias:
                        field_name = "employment_page"
                    elif "application" in field_alias:
                        field_name = "application_form"
                if not town or not field_name:
                    continue

                original_url = str(row.get("original_url") or row.get("Original URL") or "").strip()
                metadata = {
                    "validation_status": str(row.get("validation_status") or "").strip().lower(),
                    "status_code": row.get("status_code") or row.get("Status"),
                    "final_url": (row.get("final_url") or row.get("Final URL") or "").strip() or None,
                    "soft404": row.get("soft404") if "soft404" in row else row.get("Soft404"),
                    "checked_at": (row.get("checked_at") or row.get("checked_at_utc") or "").strip() or None,
                }
                lookup[(town, field_name)] = metadata
                if original_url:
                    lookup[("url", original_url.rstrip("/").lower())] = metadata

            if lookup:
                return lookup
        except Exception:
            continue
    return lookup


def _first_non_empty(row, keys):
    for key in keys:
        if key in row.index:
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if value is not None and not pd.isna(value):
                return value
    return None


def _to_int(value):
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        return bool(value)
    return str(value).strip().lower() == "true"


def _normalize_url(url):
    if not isinstance(url, str):
        return ""
    return url.strip().rstrip("/").lower()


def get_link_meta(row, url_field, prefix):
    town = str(_first_non_empty(row, ["Town", "town"]) or "").strip().lower()
    field_name = "employment_page" if prefix == "employment" else "application_form"
    lookup = LINK_HEALTH_LOOKUP.get((town, field_name), {}) if town else {}

    original_url = _first_non_empty(row, [url_field])
    if (
        not lookup
        and isinstance(original_url, str)
        and original_url.strip()
    ):
        lookup = LINK_HEALTH_LOOKUP.get(("url", _normalize_url(original_url)), {})
    final_url = _first_non_empty(row, [f"{prefix}_url_final", f"{prefix}_final_url"]) or lookup.get("final_url")
    status_code = _to_int(_first_non_empty(row, [f"{prefix}_url_status_code", f"{prefix}_status_code"]))
    if status_code is None:
        status_code = _to_int(lookup.get("status_code"))
    soft404_value = _first_non_empty(row, [f"{prefix}_url_soft404", f"{prefix}_soft404"])
    soft404 = _to_bool(soft404_value) if soft404_value is not None else _to_bool(lookup.get("soft404"))
    checked_at = _first_non_empty(row, [f"{prefix}_url_last_checked_at", f"{prefix}_last_checked_at", "checked_at"]) or lookup.get("checked_at")
    validation_status = str(
        _first_non_empty(row, [f"{prefix}_validation_status", f"{prefix}_url_validation_status", "validation_status"]) or ""
    ).strip().lower()
    if not validation_status:
        validation_status = str(lookup.get("validation_status") or "").strip().lower()

    if not isinstance(original_url, str) or not original_url.strip():
        status = "unavailable"
    elif validation_status in {"working", "verified"}:
        status = "verified"
    elif validation_status == "redirected":
        status = "redirected"
    elif validation_status in {"suspicious", "broken"}:
        status = "suspicious"
    elif soft404:
        status = "suspicious"
    elif status_code is not None and status_code >= 400:
        status = "suspicious"
    elif final_url and _normalize_url(final_url) != _normalize_url(original_url):
        status = "redirected" if status_code is None or status_code < 400 else "suspicious"
    elif status_code is not None and status_code < 400:
        status = "verified"
    else:
        status = "unverified"

    return {
        "original_url": original_url if isinstance(original_url, str) else None,
        "final_url": final_url if isinstance(final_url, str) else None,
        "status_code": status_code,
        "soft404": soft404,
        "checked_at": checked_at,
        "status": status,
    }


def status_badge_html(status):
    styles = {
        "verified": ("Verified", "#e8f5e9", "#2e7d32"),
        "redirected": ("Verified", "#e3f2fd", "#1565c0"),
        "suspicious": ("Needs review", "#fff3e0", "#ef6c00"),
        "unavailable": ("Not listed", "#f1f3f5", "#6c757d"),
        "unverified": ("Listed", "#f8f9fa", "#6c757d"),
    }
    tooltips = {
        "suspicious": "May require navigation on the municipal website",
    }
    label, bg, fg = styles.get(status, ("Listed", "#f8f9fa", "#6c757d"))
    title_attr = f' title="{tooltips.get(status)}"' if status in tooltips else ""
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:999px;'
        f'background:{bg};color:{fg};font-weight:600;font-size:0.8rem;"{title_attr}>{label}</span>'
    )


def _parse_checked_at(value):
    if value is None or value == "":
        return None
    dt = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(dt):
        return None
    return dt


def format_checked_at(value):
    dt = _parse_checked_at(value)
    if dt is None:
        return "Not available"
    now_utc = pd.Timestamp.now(tz="UTC")
    if dt.year == now_utc.year:
        return f"{dt.strftime('%b')} {dt.day}"
    return f"{dt.strftime('%b')} {dt.day}, {dt.year}"


def freshness_label(value):
    dt = _parse_checked_at(value)
    if dt is None:
        return "Listed"
    days_old = (pd.Timestamp.now(tz="UTC").normalize() - dt.normalize()).days
    days_old = max(0, int(days_old))
    if days_old <= 7:
        return "Verified"
    if days_old <= 30:
        return "Recently checked"
    return "Listed"


def verification_summary_html(meta):
    freshness = freshness_label(meta.get("checked_at"))
    checked_text = format_checked_at(meta.get("checked_at"))
    freshness_colors = {
        "Verified": "#2e7d32",
        "Recently checked": "#1565c0",
        "Listed": "#6c757d",
    }
    freshness_color = freshness_colors.get(freshness, "#6c757d")
    return (
        f'{status_badge_html(meta.get("status"))}'
        f'<div style="font-size:0.8rem;color:{freshness_color};font-weight:600;margin-top:4px;">{freshness}</div>'
        f'<div style="font-size:0.78rem;color:#555;">Last checked: {checked_text}</div>'
    )


def has_application_pdf(url):
    if not isinstance(url, str) or not url.strip():
        return False
    lowered = url.strip().lower()
    return lowered.split("?")[0].endswith(".pdf") or "pdf" in lowered


def is_third_party_platform(value):
    if not isinstance(value, str) or not value.strip():
        return False
    lowered = value.strip().lower()
    non_vendor_tokens = {"none", "n/a", "na", "manual", "unknown", "-"}
    return lowered not in non_vendor_tokens


def manual_or_pdf_process(row):
    app_url = row.get('Application Form URL')
    platform = row.get('ATS or Platform (if known)')
    if not isinstance(app_url, str) or not app_url.strip():
        return False
    return has_application_pdf(app_url) or not is_third_party_platform(platform)


def make_clickable(
    url,
    text,
    meta=None,
    color="#007bff",
    show_final_url=False,
    show_status_note=False,
):
    if meta is None:
        meta = {"status": "verified", "final_url": None}

    if pd.isna(url) or url == "" or meta.get("status") == "unavailable":
        return '<span style="color: #999; font-style: italic;">Not listed</span>'

    link_color = color if meta.get("status") in {"verified", "redirected"} else "#b85c00"
    link_html = (
        f'<a href="{url}" target="_blank" rel="noopener noreferrer" '
        f'style="color: {link_color}; text-decoration: none; font-weight: 500;">{text}</a>'
    )

    final_note = ""
    if (
        show_final_url
        and meta.get("status") == "redirected"
        and isinstance(meta.get("final_url"), str)
        and _normalize_url(meta.get("final_url")) != _normalize_url(url)
    ):
        final_note = (
            f'<div style="font-size:0.78rem;color:#555;">'
            f'Final URL: <a href="{meta["final_url"]}" target="_blank" rel="noopener noreferrer">Open</a>'
            f"</div>"
        )

    caution_note = ""
    if show_status_note and meta.get("status") == "suspicious":
        caution_note = (
            '<div style="font-size:0.78rem;color:#6c757d;">'
            "May require navigation on the municipal website."
            "</div>"
        )

    return f"{link_html}{final_note}{caution_note}"


def build_simple_directory_table(source_df):
    display_df = source_df.copy()
    display_df["Town Website"] = display_df.apply(
        lambda row: make_clickable(
            row["Town Website"],
            "Website &rarr;",
            {"status": "verified", "final_url": None},
            "#1f4788",
        ),
        axis=1,
    )
    display_df["Jobs"] = display_df.apply(
        lambda row: make_clickable(
            row["Employment Page URL"],
            "View jobs &rarr;",
            get_link_meta(row, "Employment Page URL", "employment"),
            "#007bff",
        ),
        axis=1,
    )
    display_df["Application"] = display_df.apply(
        lambda row: make_clickable(
            row["Application Form URL"],
            "Apply &rarr;",
            get_link_meta(row, "Application Form URL", "application"),
            "#28a745",
        ),
        axis=1,
    )
    display_df["Platform/System"] = display_df["ATS or Platform (if known)"].fillna("-")
    return display_df[["Town", "Town Website", "Jobs", "Application", "Platform/System"]]


def build_detailed_directory_table(source_df):
    display_df = source_df.copy()
    display_df["Town Website"] = display_df.apply(
        lambda row: make_clickable(
            row["Town Website"],
            "Visit website &rarr;",
            {"status": "verified", "final_url": None},
            "#1f4788",
        ),
        axis=1,
    )
    display_df["Employment Page"] = display_df.apply(
        lambda row: make_clickable(
            row["Employment Page URL"],
            "View jobs &rarr;",
            get_link_meta(row, "Employment Page URL", "employment"),
            "#007bff",
            show_final_url=True,
            show_status_note=True,
        ),
        axis=1,
    )
    display_df["Application Form"] = display_df.apply(
        lambda row: make_clickable(
            row["Application Form URL"],
            "Application &rarr;",
            get_link_meta(row, "Application Form URL", "application"),
            "#28a745",
            show_final_url=True,
            show_status_note=True,
        ),
        axis=1,
    )
    display_df["Employment Verification"] = display_df.apply(
        lambda row: verification_summary_html(get_link_meta(row, "Employment Page URL", "employment")),
        axis=1,
    )
    display_df["Application Verification"] = display_df.apply(
        lambda row: verification_summary_html(get_link_meta(row, "Application Form URL", "application")),
        axis=1,
    )
    display_df["Platform/System"] = display_df["ATS or Platform (if known)"].fillna("-")
    return display_df[
        [
            "Town",
            "Town Website",
            "Employment Page",
            "Employment Verification",
            "Application Form",
            "Application Verification",
            "Platform/System",
        ]
    ]


def render_directory_table(source_df, view_mode):
    if view_mode == "Simple view":
        final_display = build_simple_directory_table(source_df)
        st.markdown(final_display.to_html(escape=False, index=False), unsafe_allow_html=True)
        return

    final_display = build_detailed_directory_table(source_df)
    st.markdown(final_display.to_html(escape=False, index=False), unsafe_allow_html=True)
    st.caption(
        "Status legend: Verified = link recently confirmed. "
        "Needs review = link may require manual navigation on the municipal website. "
        "Not listed = no direct link is currently included in this dataset."
    )


def render_support_message():
    if not SUPPORT_URL:
        return
    st.caption(SUPPORT_MESSAGE)
    st.markdown(
        (
            f'<a href="{SUPPORT_URL}" target="_blank" rel="noopener noreferrer" '
            f'class="secondary-support-button">{SUPPORT_BUTTON_LABEL}</a>'
        ),
        unsafe_allow_html=True,
    )


left_col, right_col = st.columns([1.5, 1], gap="large")

with left_col:
    # Main product header
    st.markdown("""
    <h1 style='font-size: 2.4rem; font-weight: 800; margin-bottom: 0.5rem;'>
    Connecticut Municipal Employment Directory
    </h1>
    """, unsafe_allow_html=True)
    st.markdown(f"**{TOP_SUPPORTING_LINE}**")
    st.markdown(TOP_BODY_LINE)
    st.caption(TOP_MUTED_LINE)
    render_support_message()

with right_col:
    st.markdown("## Download the Full Dataset")
    st.markdown("""
    Get the complete Connecticut municipal employment directory in one structured file.

    Includes:
    - All 169 municipalities
    - Employment page links
    - Application availability
    - Platform / ATS used
    - Verification status
    - Last checked dates
    """)
    st.caption("Avoid manually opening 169 municipal websites. The full dataset gives you everything in one file.")
st.markdown("<hr style='margin: 2rem 0;'>", unsafe_allow_html=True)


# Load data
df = load_employment_data()
LINK_HEALTH_LOOKUP = load_link_health_lookup()

if not df.empty:
    if "table_view_mode" not in st.session_state:
        st.session_state["table_view_mode"] = "Simple view"

    # Sidebar - Filters
    with st.sidebar:
        for key, default_value in FILTER_DEFAULTS.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

        if hasattr(st, "logo"):
            st.logo(":material/work:", size="large")
        st.markdown("### CT Municipal Employment Directory")
        st.markdown(f"[Directory](#directory-section) | [Dataset](#dataset-section) | [Support]({SUPPORT_URL})")
        st.markdown("<hr style='margin: 0.55rem 0 0.75rem 0;border-color:#D1D5DB;'>", unsafe_allow_html=True)
        st.link_button("Download full dataset", DATASET_PURCHASE_URL, use_container_width=True)
        st.link_button(SUPPORT_BUTTON_LABEL, SUPPORT_URL, use_container_width=True)
        st.markdown("<hr style='margin: 0.75rem 0 0.75rem 0;border-color:#D1D5DB;'>", unsafe_allow_html=True)
        st.sidebar.markdown("<div style='height: 0.4rem;'></div>", unsafe_allow_html=True)
        st.caption("Free browsing tool - Full dataset available in-app")
        st.sidebar.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)
        st.header("Search & Filter")
        if st.button("Reset filters", type="secondary", use_container_width=False):
            for key, default_value in FILTER_DEFAULTS.items():
                st.session_state[key] = default_value
            st.rerun()
        st.markdown("<hr style='margin: 0.55rem 0 0.75rem 0;border-color:#D1D5DB;'>", unsafe_allow_html=True)

        # Search box
        platforms = ['All Platforms'] + sorted([p for p in df['ATS or Platform (if known)'].dropna().unique() if p])
        if st.session_state.get("selected_platform") not in platforms:
            st.session_state["selected_platform"] = "All Platforms"

        search_term = st.text_input(
            "Search by town name:",
            key="search_term",
            placeholder="e.g. Hartford, New Haven...",
            help="Type any town name to filter results"
        )

        selected_platform = st.selectbox(
            "Filter by platform:",
            platforms,
            key="selected_platform",
            help="Filter by the employment application system used"
        )

        # Availability filters
        st.subheader("Availability")
        show_job_page = st.checkbox("Has employment page", key="show_job_page")
        show_no_job_page = st.checkbox("No employment page", key="show_no_job_page")
        show_app_form = st.checkbox("Has application form", key="show_app_form")
        show_no_app_form = st.checkbox("No application form", key="show_no_app_form")

        st.subheader("Link Trust")
        verified_links_only = st.checkbox(
            "Verified links only",
            key="verified_links_only",
            help="Keep only towns where available links are shown as Verified.",
        )
        hide_suspicious_links = st.checkbox(
            "Hide \"Needs review\" statuses",
            key="hide_suspicious_links",
            help="Exclude towns where a link may require manual navigation.",
        )

        st.subheader("Advanced Use Cases")
        filter_verified_employment = st.checkbox(
            "Verified employment links",
            key="filter_verified_employment",
            help="Show municipalities where the employment link is shown as Verified.",
        )
        filter_application_pdf = st.checkbox(
            "Application PDF available",
            key="filter_application_pdf",
            help="Show municipalities with an application link that appears to be a PDF.",
        )
        filter_third_party_platform = st.checkbox(
            "Uses third-party ATS/platform",
            key="filter_third_party_platform",
            help="Show municipalities with a known ATS/platform vendor.",
        )
        filter_manual_pdf_process = st.checkbox(
            "Manual/PDF application process",
            key="filter_manual_pdf_process",
            help="Show municipalities that appear to use a manual or PDF application workflow.",
        )


        
        

    # Main content area
    # Apply filters
    filtered_df = df.copy()
    
    # Search filter
    if search_term:
        filtered_df = filtered_df[
            filtered_df['Town'].str.contains(search_term, case=False, na=False)
        ]
    
    # Platform filter
    if selected_platform != 'All Platforms':
        filtered_df = filtered_df[
            filtered_df['ATS or Platform (if known)'] == selected_platform
        ]
    
    # Availability filters
    if not (show_job_page and show_no_job_page):
        if show_job_page:
            filtered_df = filtered_df[filtered_df['Employment Page URL'].notna()]
        elif show_no_job_page:
            filtered_df = filtered_df[filtered_df['Employment Page URL'].isna()]
        else:
            filtered_df = pd.DataFrame()  # Show nothing if both unchecked
    
    if not (show_app_form and show_no_app_form):
        if show_app_form:
            filtered_df = filtered_df[filtered_df['Application Form URL'].notna()]
        elif show_no_app_form:
            filtered_df = filtered_df[filtered_df['Application Form URL'].isna()]
        else:
            filtered_df = pd.DataFrame()  # Show nothing if both unchecked

    # Trust filters (graceful fallback when metadata is missing)
    if not filtered_df.empty:
        filtered_df = filtered_df.copy()
        filtered_df['__employment_status'] = filtered_df.apply(
            lambda row: get_link_meta(row, 'Employment Page URL', 'employment')['status'],
            axis=1
        )
        filtered_df['__application_status'] = filtered_df.apply(
            lambda row: get_link_meta(row, 'Application Form URL', 'application')['status'],
            axis=1
        )
        filtered_df['__has_application_pdf'] = filtered_df['Application Form URL'].apply(has_application_pdf)
        filtered_df['__is_third_party_platform'] = filtered_df['ATS or Platform (if known)'].apply(is_third_party_platform)
        filtered_df['__manual_pdf_process'] = filtered_df.apply(manual_or_pdf_process, axis=1)

        if hide_suspicious_links:
            filtered_df = filtered_df[
                (filtered_df['__employment_status'] != 'suspicious')
                & (filtered_df['__application_status'] != 'suspicious')
            ]

        if verified_links_only:
            def row_verified_only(row):
                statuses = []
                if isinstance(row.get('Employment Page URL'), str) and row.get('Employment Page URL').strip():
                    statuses.append(row.get('__employment_status'))
                if isinstance(row.get('Application Form URL'), str) and row.get('Application Form URL').strip():
                    statuses.append(row.get('__application_status'))
                return len(statuses) > 0 and all(s in {'verified', 'redirected'} for s in statuses)

            filtered_df = filtered_df[filtered_df.apply(row_verified_only, axis=1)]

        if filter_verified_employment:
            filtered_df = filtered_df[filtered_df['__employment_status'].isin(['verified', 'redirected'])]
        if filter_application_pdf:
            filtered_df = filtered_df[filtered_df['__has_application_pdf']]
        if filter_third_party_platform:
            filtered_df = filtered_df[filtered_df['__is_third_party_platform']]
        if filter_manual_pdf_process:
            filtered_df = filtered_df[filtered_df['__manual_pdf_process']]
    
    # Statistics
    st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    municipalities_count = len(filtered_df)
    job_pages_count = len(filtered_df[filtered_df['Employment Page URL'].notna()])
    applications_count = len(filtered_df[filtered_df['Application Form URL'].notna()])
    platforms_count = filtered_df['ATS or Platform (if known)'].nunique()

    col1.metric("Municipalities", municipalities_count)
    col2.metric("With Job Pages", job_pages_count)
    col3.metric("With Applications", applications_count)
    col4.metric("Platforms Used", platforms_count)
    st.caption(STATS_LINE_ONE)
    st.caption(STATS_LINE_TWO)
    st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

    # Active filter summary
    if search_term or selected_platform != 'All Platforms':
        filters_applied = []
        if search_term:
            filters_applied.append(f'"{search_term}"')
        if selected_platform != 'All Platforms':
            filters_applied.append(f'Platform: {selected_platform}')

        st.caption(f"Active filters: {', '.join(filters_applied)}")

    st.markdown("<hr style='margin: 2rem 0;'>", unsafe_allow_html=True)
    st.markdown('<div id="directory-section"></div>', unsafe_allow_html=True)
    st.markdown("## Browse the Free Directory")
    st.markdown(BROWSE_BODY_LINE)
    st.caption("Working across multiple towns? Copying this manually gets tedious quickly.")
    st.caption(f"{len(filtered_df)} result{'s' if len(filtered_df) != 1 else ''}")
    table_view_mode = st.radio(
        "Table view",
        ["Simple view", "Detailed view"],
        key="table_view_mode",
        horizontal=True,
    )
    
    if len(filtered_df) == 0:
        st.warning("No municipalities match your current filters. Try adjusting your search criteria.")
    else:
        render_directory_table(filtered_df, table_view_mode)

    if len(filtered_df) > 0:
        st.warning("If you're working across multiple towns, doing this manually gets slow quickly.")
        render_support_message()

    st.markdown("<hr style='margin: 2rem 0;'>", unsafe_allow_html=True)
    st.markdown('<div id="dataset-section"></div>', unsafe_allow_html=True)
    st.markdown(f"## {BOTTOM_DATASET_HEADER}")
    st.markdown("""
    Avoid manually opening, checking, and copying from 169 separate municipal websites.

    The full dataset gives you the complete directory in one structured file for recruiting, outreach, consulting, and research.
    """)
    st.link_button(
        "Download Full Dataset ($49)",
        DATASET_PURCHASE_URL,
        type="primary",
        use_container_width=True,
    )
    st.caption("Without the dataset, this means opening, checking, and copying from 169 separate sites.")

    st.info("""
Some municipalities use third-party hiring systems where the job page itself serves as the application.

In those cases, a separate application form may appear as not listed.

This directory reflects how municipal hiring systems actually operate across Connecticut.
""")

else:
    st.error("Unable to load employment data. Please check that the data file exists.")
