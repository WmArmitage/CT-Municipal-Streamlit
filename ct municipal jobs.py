import streamlit as st
import pandas as pd
import json
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Connecticut Municipal Employment Portal",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f4788;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
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
    .donate-button {
        background: #28a745;
        color: white;
        padding: 1rem 2rem;
        border-radius: 8px;
        text-align: center;
        font-weight: bold;
        margin: 2rem 0;
    }
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# Load data
@st.cache_data
def load_employment_data():
    """Load the CT municipal employment data from JSON file"""
    try:
        with open('CT_Municipal_Employment_Pages.json', 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        return df
    except FileNotFoundError:
        st.error("Data file not found. Please ensure CT_Municipal_Employment_Pages.json is in the same directory.")
        return pd.DataFrame()

# Initialize session state for user subscriptions
if 'subscribers' not in st.session_state:
    st.session_state.subscribers = []

# Main header
st.markdown('<div class="main-header">🏛️ Connecticut Municipal Employment Portal</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Find job opportunities across all 169 Connecticut municipalities in one place</div>', unsafe_allow_html=True)

# Load data
df = load_employment_data()

if not df.empty:
    # Sidebar - Filters and User Signup
    with st.sidebar:
        st.header("🔍 Search & Filter")
        
        # Search box
        search_term = st.text_input("Search by town name:", placeholder="e.g. Hartford, New Haven...")
        
        # Platform filter
        platforms = ['All'] + sorted([p for p in df['ATS or Platform (if known)'].dropna().unique() if p])
        selected_platform = st.selectbox("Filter by platform:", platforms)
        
        # Filter for towns with/without job pages
        job_page_filter = st.radio(
            "Show towns:",
            ["All", "With job pages only", "Without job pages only"]
        )
        
        st.markdown("---")
        
        # User signup form
        st.header("📧 Get Job Alerts")
        st.write("Subscribe to receive notifications when new jobs are posted in your selected towns.")
        
        with st.form("subscribe_form"):
            user_name = st.text_input("Your Name:")
            user_email = st.text_input("Email Address:")
            
            # Multi-select for towns
            selected_towns = st.multiselect(
                "Select towns to follow:",
                options=sorted(df['Town'].tolist()),
                help="Choose one or more towns you're interested in"
            )
            
            subscribe_button = st.form_submit_button("Subscribe for Updates")
            
            if subscribe_button:
                if user_email and user_name and selected_towns:
                    subscriber = {
                        'name': user_name,
                        'email': user_email,
                        'towns': selected_towns,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    st.session_state.subscribers.append(subscriber)
                    st.success(f"✅ Thanks {user_name}! You're subscribed to {len(selected_towns)} town(s).")
                else:
                    st.error("Please fill in all fields.")
        
        st.markdown("---")
        
        # Donation section
        st.header("💝 Support This Project")
        st.write("Help us maintain and improve this free resource!")
        
        # Replace these with your actual donation links
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("[☕ Buy Me a Coffee](https://buymeacoffee.com/yourlink)")
        with col2:
            st.markdown("[💳 PayPal](https://paypal.me/yourlink)")
        
        st.markdown("---")
        
        # Download subscribers (admin feature)
        if st.session_state.subscribers:
            st.header("📊 Admin")
            if st.button("Download Subscribers CSV"):
                subscribers_df = pd.DataFrame(st.session_state.subscribers)
                csv = subscribers_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"subscribers_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
                st.info(f"Total subscribers: {len(st.session_state.subscribers)}")

    # Main content area
    # Apply filters
    filtered_df = df.copy()
    
    # Search filter
    if search_term:
        filtered_df = filtered_df[
            filtered_df['Town'].str.contains(search_term, case=False, na=False)
        ]
    
    # Platform filter
    if selected_platform != 'All':
        filtered_df = filtered_df[
            filtered_df['ATS or Platform (if known)'] == selected_platform
        ]
    
    # Job page filter
    if job_page_filter == "With job pages only":
        filtered_df = filtered_df[filtered_df['Employment Page URL'].notna()]
    elif job_page_filter == "Without job pages only":
        filtered_df = filtered_df[filtered_df['Employment Page URL'].isna()]
    
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
    
    # Display results
    st.subheader(f"📋 Showing {len(filtered_df)} Results")
    
    # Prepare display dataframe with clickable links
    display_df = filtered_df.copy()
    
    # Function to create clickable links
    def make_clickable(url, text):
        if pd.isna(url) or url == '':
            return 'N/A'
        return f'<a href="{url}" target="_blank">{text}</a>'
    
    # Create display columns
    display_df['Jobs Page'] = display_df.apply(
        lambda row: make_clickable(row['Employment Page URL'], '🔗 View Jobs'), 
        axis=1
    )
    display_df['Application'] = display_df.apply(
        lambda row: make_clickable(row['Application Form URL'], '📄 Get Form'), 
        axis=1
    )
    display_df['Website'] = display_df.apply(
        lambda row: make_clickable(row['Town Website'], '🌐 Visit Site'), 
        axis=1
    )
    
    # Select and reorder columns for display
    display_cols = ['Town', 'Jobs Page', 'Application', 'Website', 'ATS or Platform (if known)']
    display_df = display_df[display_cols]
    display_df.columns = ['Town', 'Jobs Page', 'Application Form', 'Town Website', 'Platform']
    
    # Display as HTML table for clickable links
    st.markdown(
        display_df.to_html(escape=False, index=False), 
        unsafe_allow_html=True
    )
    
    # Download filtered results
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"ct_municipal_jobs_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    
    with col2:
        # Export to JSON
        json_str = filtered_df.to_json(orient='records', indent=2)
        st.download_button(
            label="📥 Download as JSON",
            data=json_str,
            file_name=f"ct_municipal_jobs_{datetime.now().strftime('%Y%m%d')}.json",
            mime="application/json"
        )
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 2rem 0;'>
        <p><strong>Connecticut Municipal Employment Portal</strong></p>
        <p>Data is updated regularly. Please verify job postings on official town websites.</p>
        <p>For questions or to report issues, contact us at: <a href='mailto:your-email@example.com'>your-email@example.com</a></p>
    </div>
    """, unsafe_allow_html=True)

else:
    st.error("Unable to load employment data. Please check that the data file exists.")
