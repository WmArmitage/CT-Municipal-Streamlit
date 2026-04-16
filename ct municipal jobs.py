import streamlit as st
import pandas as pd
import json

# Page configuration
st.set_page_config(
    page_title="Connecticut Municipal Employment Directory",
    page_icon="ðŸ›ï¸",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.8rem;
        font-weight: bold;
        color: #1f4788;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.3rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
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
    .donate-section {
        background: #f8f9fa;
        padding: 2rem;
        border-radius: 12px;
        text-align: center;
        margin: 2rem 0;
        border: 2px solid #e9ecef;
    }
    .donate-button {
        display: inline-block;
        padding: 12px 30px;
        margin: 10px;
        border-radius: 8px;
        text-decoration: none;
        font-weight: 600;
        font-size: 1.1rem;
        transition: transform 0.2s;
    }
    .donate-button:hover {
        transform: translateY(-2px);
    }
    .kofi-button {
        background: #29abe0;
        color: white;
    }

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
        import urllib.request
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Unable to load employment data from GitHub: {str(e)}")
        return pd.DataFrame()


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
    original_url = _first_non_empty(row, [url_field])
    final_url = _first_non_empty(row, [f"{prefix}_url_final", f"{prefix}_final_url"])
    status_code = _to_int(_first_non_empty(row, [f"{prefix}_url_status_code", f"{prefix}_status_code"]))
    soft404 = _to_bool(_first_non_empty(row, [f"{prefix}_url_soft404", f"{prefix}_soft404"]))
    checked_at = _first_non_empty(row, [f"{prefix}_url_last_checked_at", f"{prefix}_last_checked_at", "checked_at"])
    validation_status = str(
        _first_non_empty(row, [f"{prefix}_validation_status", f"{prefix}_url_validation_status", "validation_status"]) or ""
    ).strip().lower()

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
        "redirected": ("Redirected", "#e3f2fd", "#1565c0"),
        "suspicious": ("Suspicious", "#fff3e0", "#ef6c00"),
        "unavailable": ("Unavailable", "#f1f3f5", "#6c757d"),
        "unverified": ("Unverified", "#f8f9fa", "#6c757d"),
    }
    label, bg, fg = styles.get(status, ("Unverified", "#f8f9fa", "#6c757d"))
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:999px;'
        f'background:{bg};color:{fg};font-weight:600;font-size:0.8rem;">{label}</span>'
    )


def format_checked_at(value):
    if value is None or value == "":
        return "-"
    dt = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(dt):
        return str(value)
    return dt.strftime("%Y-%m-%d")

# Main header
st.markdown('<div class="main-header">ðŸ›ï¸ Connecticut Municipal Employment Directory</div>', unsafe_allow_html=True)
st.markdown("""
<div class="sub-header">
    Quick access to employment opportunities across all 169 Connecticut municipalities
</div>
<div class="sub-note">
    â€œNot Availableâ€ does not necessarily indicate missing or broken information.
    Connecticut municipalities use a wide variety of website structures and hiring systems,
    including third-party applicant tracking platforms where the employment page itself serves as the application.
    In these cases, a separate application form does not exist and will always appear as â€œNot Available.â€
    <br><br>
    In other instances, data may be unavailable due to non-standard page layouts, dynamically generated content,
    or frequent structural changes on municipal websites.
</div>
""", unsafe_allow_html=True)


# Load data
df = load_employment_data()

if not df.empty:
    # Sidebar - Filters
    with st.sidebar:
        st.header("ðŸ” Search & Filter")
        
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
            help="Keep only towns where available links are verified or redirected.",
        )
        hide_suspicious_links = st.checkbox(
            "Hide suspicious links",
            value=False,
            help="Exclude towns with suspicious/broken employment or application links.",
        )
        
        st.markdown("---")
        
        # About section
        st.header("â„¹ï¸ About")
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
            <strong>ðŸ” Active Filters:</strong> {', '.join(filters_applied)}
        </div>
        """, unsafe_allow_html=True)
    
    # Display results
    st.subheader(f"ðŸ“‹ {len(filtered_df)} Result{'s' if len(filtered_df) != 1 else ''}")
    
    if len(filtered_df) == 0:
        st.warning("No municipalities match your current filters. Try adjusting your search criteria.")
    else:
        # Prepare display dataframe with clickable links
        display_df = filtered_df.copy()
        
        # Function to create clickable links with trust context
        def make_clickable(url, text, meta, color="#007bff"):
            if pd.isna(url) or url == '' or meta['status'] == 'unavailable':
                return '<span style="color: #999; font-style: italic;">Not Available</span>'

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
                caution_note = '<div style="font-size:0.78rem;color:#b85c00;">Use caution: link may be outdated.</div>'

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
        display_df['Employment Status'] = display_df.apply(
            lambda row: status_badge_html(get_link_meta(row, 'Employment Page URL', 'employment')['status']),
            axis=1
        )
        display_df['Employment Last Verified'] = display_df.apply(
            lambda row: format_checked_at(get_link_meta(row, 'Employment Page URL', 'employment')['checked_at']),
            axis=1
        )
        display_df['Application Status'] = display_df.apply(
            lambda row: status_badge_html(get_link_meta(row, 'Application Form URL', 'application')['status']),
            axis=1
        )
        display_df['Application Last Verified'] = display_df.apply(
            lambda row: format_checked_at(get_link_meta(row, 'Application Form URL', 'application')['checked_at']),
            axis=1
        )
        
        # Handle platform display
        display_df['Platform/System'] = display_df['ATS or Platform (if known)'].fillna('â€”')
        
        # Select columns for display
        final_display = display_df[
            [
                'Town',
                'Town Website',
                'Employment Page',
                'Employment Status',
                'Employment Last Verified',
                'Application Form',
                'Application Status',
                'Application Last Verified',
                'Platform/System'
            ]
        ]
        
        # Display as HTML table for clickable links
        st.markdown(
            final_display.to_html(escape=False, index=False), 
            unsafe_allow_html=True
        )
        st.caption(
            "Status legend: Verified = recently working, Redirected = valid redirect, "
            "Suspicious = broken/soft-404 or uncertain, Unavailable = no link in dataset."
        )
    
    # --- Donation button CSS (define once) ---
    st.markdown("""
    <style>
    .donate-section {
        text-align: center;
        padding: 2rem 1rem;
    }

    .donate-button {
        display: inline-block;
        padding: 0.9rem 1.4rem;
        margin: 0.5rem;
        border-radius: 8px;
        font-weight: 700;
        font-size: 1rem;
        text-decoration: none !important;
        color: #ffffff !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.15);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }

    .donate-button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }

    .kofi-button {
        background-color: #29abe0;
    }

 
    }
    </style>
    """, unsafe_allow_html=True)

    
    
    
    # Large donation section at bottom
    st.markdown("---")
    st.markdown("""
    <div class="donate-section">
        <h2 style="color: #1f4788; margin-bottom: 1rem;"> Support This Free Resource</h2>
        <p style="font-size: 1.1rem; color: #444; max-width: 700px; margin: 0 auto 1.5rem;">
            This directory is independently built and maintained. 
                If you found it useful, youâ€™re welcome to support its continued development with a donation.
        </p>
        <div>
            <a href="https://ko-fi.com/wmarmitage" target="_blank" class="donate-button kofi-button">
                â˜• Support on Ko-fi
            </a>
        </div>
        <p style="font-size: 0.9rem; color: #444; margin-top: 1.5rem;">
            Donations support the time and effort required to maintain and improve this directory.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Streamlit â€œFor Agenciesâ€ Section (drop-in)
    st.markdown("---")
    st.markdown("### ðŸ›ï¸ For Agencies, Researchers, and Vendors")

    st.markdown(
    """
    This directory is provided as a free public resource for job seekers.

    For professional, research, or commercial use, a **licensed dataset snapshot**
    of the Connecticut Municipal Employment Directory is available.
    
    Purchases are handled securely through Ko-fi and include immediate access to
    the dataset, documentation, and license.
    """
)

    st.link_button(
    "ðŸ“¦ Purchase Licensed Dataset",
    "https://ko-fi.com/s/814c806c0b"
)

    st.caption(
    "Ko-fi provides receipts and handles applicable taxes for digital purchases."
    )



    # Footer
    st.markdown("---")

    st.caption("Employment page links may change periodically as municipalities update their websites.")
    
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 2rem 0;'>
    <p><strong>Connecticut Municipal Employment Directory</strong></p>
    <p style="margin: 0.5rem 0;">
        Coverage includes all 169 Connecticut municipalities
    </p>

    <!--
    <p style="margin: 1rem 0 0.5rem;">
        Found a broken link or outdated information?
    </p>
    <p style="margin: 0;">
        Submit with this form: <a href='https://tally.so/r/eqR5Dq' style='color: #007bff;'>Form</a>
    </p>
    -->

    <p style='font-size: 0.85rem; color: #999; margin-top: 1.5rem;'>
        Applicants should always confirm details directly on official municipal websites before applying
    </p>
    </div>
    """, unsafe_allow_html=True)

else:
    st.error("Unable to load employment data. Please check that the data file exists.")

