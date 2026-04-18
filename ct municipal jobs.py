import streamlit as st
import pandas as pd
import json
import csv
import urllib.request

# Page configuration
st.set_page_config(
    page_title="Connecticut Municipal Employment Directory",
    page_icon="CT",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATASET_PURCHASE_URL = "https://ko-fi.com/s/814c806c0b"

# Custom CSS
st.markdown("""
<style>
    .stats-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 1rem;
    }
    .stats-box h2 {
        margin: 0;
        font-size: 2.5rem;
    }
    .stats-box p {
        margin: 0.5rem 0 0 0;
        font-size: 0.95rem;
    }
    .search-info {
        background: #e3f2fd;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1.5rem;
        border-left: 4px solid #2196f3;
    }
</style>
""", unsafe_allow_html=True)

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
        "suspicious": ("Check link", "#fff3e0", "#ef6c00"),
        "unavailable": ("Unavailable", "#f1f3f5", "#6c757d"),
        "unverified": ("Available", "#f8f9fa", "#6c757d"),
    }
    tooltips = {
        "suspicious": "Link may require manual verification",
    }
    label, bg, fg = styles.get(status, ("Available", "#f8f9fa", "#6c757d"))
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
        return "Available"
    days_old = (pd.Timestamp.now(tz="UTC").normalize() - dt.normalize()).days
    days_old = max(0, int(days_old))
    if days_old <= 7:
        return "Verified"
    if days_old <= 30:
        return "Recently checked"
    return "Available"


def verification_summary_html(meta):
    freshness = freshness_label(meta.get("checked_at"))
    checked_text = format_checked_at(meta.get("checked_at"))
    freshness_colors = {
        "Verified": "#2e7d32",
        "Recently checked": "#1565c0",
        "Available": "#6c757d",
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


# Main header
st.title("Connecticut Municipal Employment Directory")
st.markdown("""
Quickly access employment opportunities across all 169 Connecticut municipalities.

Search by town, filter by hiring platform, and go directly to official job pages and application forms.

If you need the full structured dataset for research, outreach, or internal use, a complete export is available below.
""")

st.markdown("## Get the Full Dataset")
st.markdown("""
Download the complete Connecticut Municipal Employment Directory as a clean, ready-to-use dataset.

Includes:
- All 169 municipalities
- Direct employment page links
- Application form availability
- Platform / ATS identification
- Link verification status and last checked dates

Designed for:
- Sales and outreach
- Policy and research
- Recruiting and job aggregation
- Data analysis and automation
""")
st.link_button("Get Full Dataset ($25)", DATASET_PURCHASE_URL)


# Load data
df = load_employment_data()
LINK_HEALTH_LOOKUP = load_link_health_lookup()

if not df.empty:
    # Sidebar - Filters
    with st.sidebar:
        st.header("Search & Filter")
        
        # Search box
        search_term = st.text_input(
            "Search by town name:", 
            placeholder="e.g. Hartford, New Haven...",
            help="Type any town name to filter results"
        )
        
        # Platform filter
        platforms = ['All Platforms'] + sorted([p for p in df['ATS or Platform (if known)'].dropna().unique() if p])
        selected_platform = st.selectbox(
            "Filter by platform:", 
            platforms,
            help="Filter by the employment application system used"
        )
        
        # Availability filters
        st.subheader("Availability")
        show_job_page = st.checkbox("Has employment page", value=True)
        show_no_job_page = st.checkbox("No employment page", value=True)
        show_app_form = st.checkbox("Has application form", value=True)
        show_no_app_form = st.checkbox("No application form", value=True)

        st.subheader("Link Trust")
        verified_links_only = st.checkbox(
            "Verified links only",
            value=False,
            help="Keep only towns where available links are shown as Verified.",
        )
        hide_suspicious_links = st.checkbox(
            "Hide \"Check link\" statuses",
            value=False,
            help="Exclude towns where a link may require manual verification.",
        )

        st.subheader("Advanced Use Cases")
        filter_verified_employment = st.checkbox(
            "Verified employment links",
            value=False,
            help="Show municipalities where the employment link is shown as Verified.",
        )
        filter_application_pdf = st.checkbox(
            "Application PDF available",
            value=False,
            help="Show municipalities with an application link that appears to be a PDF.",
        )
        filter_third_party_platform = st.checkbox(
            "Uses third-party ATS/platform",
            value=False,
            help="Show municipalities with a known ATS/platform vendor.",
        )
        filter_manual_pdf_process = st.checkbox(
            "Manual/PDF application process",
            value=False,
            help="Show municipalities that appear to use a manual or PDF application workflow.",
        )
        
        st.markdown("---")
        
        # About section
        st.header("About")
        st.markdown("""
        This directory provides quick access to employment resources for all Connecticut municipalities.
        
        **Features:**
        - Direct links to town websites
        - Employment/career pages
        - Downloadable application forms
        - Platform information
        
        
        """)
        
        st.markdown("---")
        

        
        

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
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="stats-box">
            <h2>{len(filtered_df)}</h2>
            <p>Municipalities</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        with_jobs = len(filtered_df[filtered_df['Employment Page URL'].notna()])
        st.markdown(f"""
        <div class="stats-box">
            <h2>{with_jobs}</h2>
            <p>With Job Pages</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        with_apps = len(filtered_df[filtered_df['Application Form URL'].notna()])
        st.markdown(f"""
        <div class="stats-box">
            <h2>{with_apps}</h2>
            <p>With Applications</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        platforms_count = filtered_df['ATS or Platform (if known)'].nunique()
        st.markdown(f"""
        <div class="stats-box">
            <h2>{platforms_count}</h2>
            <p>Platforms Used</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Search info box
    if search_term or selected_platform != 'All Platforms':
        filters_applied = []
        if search_term:
            filters_applied.append(f'"{search_term}"')
        if selected_platform != 'All Platforms':
            filters_applied.append(f'Platform: {selected_platform}')
        
        st.markdown(f"""
        <div class="search-info">
            <strong>Active Filters:</strong> {', '.join(filters_applied)}
        </div>
        """, unsafe_allow_html=True)
    
    # Display results
    st.markdown("## Browse Municipal Employment Links")
    st.caption(f"{len(filtered_df)} result{'s' if len(filtered_df) != 1 else ''}")
    
    if len(filtered_df) == 0:
        st.warning("No municipalities match your current filters. Try adjusting your search criteria.")
    else:
        # Prepare display dataframe with clickable links
        display_df = filtered_df.copy()
        
        # Function to create clickable links with trust context
        def make_clickable(url, text, meta, color="#007bff"):
            if pd.isna(url) or url == '' or meta['status'] == 'unavailable':
                return '<span style="color: #999; font-style: italic;">Unavailable</span>'

            link_color = color if meta['status'] in {'verified', 'redirected'} else '#b85c00'
            link_html = (
                f'<a href="{url}" target="_blank" rel="noopener noreferrer" '
                f'style="color: {link_color}; text-decoration: none; font-weight: 500;">{text} -></a>'
            )

            final_note = ""
            if (
                meta['status'] == 'redirected'
                and isinstance(meta['final_url'], str)
                and _normalize_url(meta['final_url']) != _normalize_url(url)
            ):
                final_note = (
                    f'<div style="font-size:0.78rem;color:#555;">'
                    f'Final URL: <a href="{meta["final_url"]}" target="_blank" rel="noopener noreferrer">Open</a>'
                    f'</div>'
                )

            caution_note = ""
            if meta['status'] == 'suspicious':
                caution_note = '<div style="font-size:0.78rem;color:#6c757d;">Link may require manual verification.</div>'

            return f"{link_html}{final_note}{caution_note}"
        # Create display columns
        display_df['Town Website'] = display_df.apply(
            lambda row: make_clickable(
                row['Town Website'],
                'Visit Website',
                {'status': 'verified', 'final_url': None},
                '#1f4788'
            ),
            axis=1
        )
        display_df['Employment Page'] = display_df.apply(
            lambda row: make_clickable(
                row['Employment Page URL'],
                'View Jobs',
                get_link_meta(row, 'Employment Page URL', 'employment'),
                '#007bff'
            ),
            axis=1
        )
        display_df['Application Form'] = display_df.apply(
            lambda row: make_clickable(
                row['Application Form URL'],
                'Download Form',
                get_link_meta(row, 'Application Form URL', 'application'),
                '#28a745'
            ),
            axis=1
        )
        display_df['Employment Verification'] = display_df.apply(
            lambda row: verification_summary_html(get_link_meta(row, 'Employment Page URL', 'employment')),
            axis=1
        )
        display_df['Application Verification'] = display_df.apply(
            lambda row: verification_summary_html(get_link_meta(row, 'Application Form URL', 'application')),
            axis=1
        )
        
        # Handle platform display
        display_df['Platform/System'] = display_df['ATS or Platform (if known)'].fillna('-')
        
        # Select columns for display
        final_display = display_df[
            [
                'Town',
                'Town Website',
                'Employment Page',
                'Employment Verification',
                'Application Form',
                'Application Verification',
                'Platform/System'
            ]
        ]
        
        # Display as HTML table for clickable links
        st.markdown(
            final_display.to_html(escape=False, index=False), 
            unsafe_allow_html=True
        )
        st.caption(
            "Status legend: Verified = link confirmed and includes valid redirects, "
            "Available = link is listed but may not have recent verification, "
            "Check link = link may require manual verification, Unavailable = no link in dataset."
        )

    st.info("""
Some municipalities use third-party hiring systems where the job page itself serves as the application.

In these cases, a separate application form will appear as "Unavailable."

This directory reflects how municipal hiring systems actually operate across Connecticut.
""")

    st.markdown("## Need the Full Dataset?")
    st.markdown("""
Skip manual searching and get the complete dataset covering all 169 municipalities in one structured file.
""")
    st.link_button("Download Full Dataset", DATASET_PURCHASE_URL)

else:
    st.error("Unable to load employment data. Please check that the data file exists.")

