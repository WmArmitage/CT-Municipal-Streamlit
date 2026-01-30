import streamlit as st
import pandas as pd
import json

# Page configuration
st.set_page_config(
    page_title="Connecticut Municipal Employment Directory",
    page_icon="🏛️",
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
    .paypal-button {
        background: #0070ba;
        color: white;
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

# Main header
st.markdown('<div class="main-header">🏛️ Connecticut Municipal Employment Directory</div>', unsafe_allow_html=True)
st.markdown("""
<div class="sub-header">
    Quick access to employment opportunities across all 169 Connecticut municipalities
</div>
<div class="sub-note">
    “Not Available” does not necessarily indicate missing or broken information.
    Connecticut municipalities use a wide variety of website structures and hiring systems,
    including third-party applicant tracking platforms where the employment page itself serves as the application.
    In these cases, a separate application form does not exist and will always appear as “Not Available.”
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
        st.header("🔍 Search & Filter")
        
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
        
        st.markdown("---")
        
        # About section
        st.header("ℹ️ About")
        st.markdown("""
        This directory provides quick access to employment resources for all Connecticut municipalities.
        
        **Features:**
        - Direct links to town websites
        - Employment/career pages
        - Downloadable application forms
        - Platform information
        
        
        """)
        
        st.markdown("---")
        
        # Support section
        st.header("💝 Support This Project")
        st.markdown("""
        This directory is **free to use**. If you find it helpful, consider supporting its maintenance and development!
        """)
        
        # Donation buttons
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            <a href="https://ko-fi.com/wmarmitage" target="_blank" style="text-decoration: none;">
                <div style="background: #29abe0; color: white; padding: 10px; border-radius: 6px; text-align: center; font-weight: 600; margin-bottom: 10px;">
                    ☕ Ko-fi
                </div>
            </a>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <a href="https://www.paypal.me/WmArmitage" target="_blank" style="text-decoration: none;">
                <div style="background: #0070ba; color: white; padding: 10px; border-radius: 6px; text-align: center; font-weight: 600; margin-bottom: 10px;">
                    💳 PayPal
                </div>
            </a>
            """, unsafe_allow_html=True)

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
            <strong>🔍 Active Filters:</strong> {', '.join(filters_applied)}
        </div>
        """, unsafe_allow_html=True)
    
    # Display results
    st.subheader(f"📋 {len(filtered_df)} Result{'s' if len(filtered_df) != 1 else ''}")
    
    if len(filtered_df) == 0:
        st.warning("No municipalities match your current filters. Try adjusting your search criteria.")
    else:
        # Prepare display dataframe with clickable links
        display_df = filtered_df.copy()
        
        # Function to create clickable links
        def make_clickable(url, text, color="#007bff"):
            if pd.isna(url) or url == '':
                return '<span style="color: #999; font-style: italic;">Not Available</span>'
            return f'<a href="{url}" target="_blank" rel="noopener noreferrer" style="color: {color}; text-decoration: none; font-weight: 500;">{text} →</a>'
        
        # Create display columns
        display_df['Town Website'] = display_df.apply(
            lambda row: make_clickable(row['Town Website'], 'Visit Website', '#1f4788'), 
            axis=1
        )
        display_df['Employment Page'] = display_df.apply(
            lambda row: make_clickable(row['Employment Page URL'], 'View Jobs', '#007bff'), 
            axis=1
        )
        display_df['Application Form'] = display_df.apply(
            lambda row: make_clickable(row['Application Form URL'], 'Download Form', '#28a745'), 
            axis=1
        )
        
        # Handle platform display
        display_df['Platform/System'] = display_df['ATS or Platform (if known)'].fillna('—')
        
        # Select columns for display
        final_display = display_df[['Town', 'Town Website', 'Employment Page', 'Application Form', 'Platform/System']]
        
        # Display as HTML table for clickable links
        st.markdown(
            final_display.to_html(escape=False, index=False), 
            unsafe_allow_html=True
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

    .paypal-button {
        background-color: #0070ba;
    }
    </style>
    """, unsafe_allow_html=True)

    
    
    
    # Large donation section at bottom
    st.markdown("---")
    st.markdown("""
    <div class="donate-section">
        <h2 style="color: #1f4788; margin-bottom: 1rem;">🙏 Support This Free Resource</h2>
        <p style="font-size: 1.1rem; color: #444; max-width: 700px; margin: 0 auto 1.5rem;">
            This directory is independently built and maintained. 
                If you found it useful, you’re welcome to support its continued development with a donation.
        </p>
        <div>
            <a href="https://ko-fi.com/wmarmitage" target="_blank" class="donate-button kofi-button">
                ☕ Support on Ko-fi
            </a>
            <a href="https://www.paypal.me/WmArmitage" target="_blank" class="donate-button paypal-button">
                💳 Donate via PayPal
            </a>
        </div>
        <p style="font-size: 0.9rem; color: #444; margin-top: 1.5rem;">
            Donations support the time and effort required to maintain and improve this directory.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 2rem 0;'>
        <p><strong>Connecticut Municipal Employment Directory</strong></p>
        <p style="margin: 0.5rem 0;">
            Coverage includes all 169 Connecticut municipalities
        </p>
        <p style="margin: 1rem 0 0.5rem;">
            Found a broken link or outdated information?
        </p>
        <p style="margin: 0;">
            Submit with this form: <a href='https://tally.so/r/eqR5Dq' style='color: #007bff;'>Tally Form</a>
        </p>
        <p style='font-size: 0.85rem; color: #999; margin-top: 1.5rem;'>
            Applicants should always confirm details directly on official municipal websites before applying
        </p>
    </div>
    """, unsafe_allow_html=True)

else:
    st.error("Unable to load employment data. Please check that the data file exists.")
