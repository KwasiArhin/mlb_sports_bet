// MLB Pitcher Dashboard JavaScript

class PitcherDashboard {
    constructor() {
        this.pitchers = [];
        this.filteredPitchers = [];
        this.currentSort = { field: 'composite_score', direction: 'desc' };
        
        this.init();
    }
    
    init() {
        // Load initial data
        this.loadPitchers();
        
        // Setup event listeners
        this.setupEventListeners();
        
        // Setup table sorting
        this.setupTableSorting();
        
        console.log('🚀 Pitcher Dashboard initialized');
    }
    
    loadPitchers() {
        // Extract pitcher data from the rendered template
        const tableRows = document.querySelectorAll('#pitcher-table tbody tr');
        this.pitchers = Array.from(tableRows).map((row, index) => {
            const cells = row.querySelectorAll('td');
            return {
                rank: parseInt(cells[0].textContent),
                pitcher_name: cells[1].querySelector('.name').textContent,
                team: cells[2].querySelector('.team-badge').textContent,
                composite_score: parseFloat(cells[3].querySelector('.score-value').textContent),
                grade: cells[4].querySelector('.grade-badge').textContent,
                tier: cells[5].querySelector('.tier-badge').textContent,
                whip: this.parseNumeric(cells[6].textContent),
                fip: this.parseNumeric(cells[7].textContent),
                siera: this.parseNumeric(cells[8].textContent),
                csw_rate: this.parseNumeric(cells[9].textContent),
                xera: this.parseNumeric(cells[10].textContent),
                xfip: this.parseNumeric(cells[11].textContent),
                xwoba: this.parseNumeric(cells[12].textContent),
                xba: this.parseNumeric(cells[13].textContent),
                xslg: this.parseNumeric(cells[14].textContent),
                stuff_plus: this.parseNumeric(cells[15].textContent),
                innings_pitched: this.parseNumeric(cells[16].textContent),
                strengths: cells[17].textContent,
                weaknesses: cells[18].textContent,
                element: row
            };
        });
        
        this.filteredPitchers = [...this.pitchers];
        console.log(`📊 Loaded ${this.pitchers.length} pitchers`);
    }
    
    parseNumeric(value) {
        const num = parseFloat(value);
        return isNaN(num) ? null : num;
    }
    
    setupEventListeners() {
        // Filter controls
        const tierFilter = document.getElementById('tier-filter');
        const teamFilter = document.getElementById('team-filter');
        const sortBy = document.getElementById('sort-by');
        const searchInput = document.getElementById('pitcher-search');
        const exportBtn = document.getElementById('export-btn');
        
        if (tierFilter) {
            tierFilter.addEventListener('change', () => this.applyFilters());
        }
        
        if (teamFilter) {
            teamFilter.addEventListener('change', () => this.applyFilters());
        }
        
        if (sortBy) {
            sortBy.addEventListener('change', (e) => {
                this.sortPitchers(e.target.value);
            });
        }
        
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.searchPitchers(e.target.value);
            });
        }
        
        if (exportBtn) {
            exportBtn.addEventListener('click', () => this.exportData());
        }
    }
    
    setupTableSorting() {
        const headers = document.querySelectorAll('#pitcher-table th[data-sort]');
        
        headers.forEach(header => {
            header.addEventListener('click', () => {
                const field = header.getAttribute('data-sort');
                this.sortPitchers(field);
            });
            
            // Add cursor pointer style
            header.style.cursor = 'pointer';
            
            // Add sort indicator
            const sortIcon = document.createElement('i');
            sortIcon.className = 'fas fa-sort sort-icon';
            sortIcon.style.marginLeft = '0.5rem';
            sortIcon.style.opacity = '0.5';
            header.appendChild(sortIcon);
        });
    }
    
    applyFilters() {
        const tierFilter = document.getElementById('tier-filter').value;
        const teamFilter = document.getElementById('team-filter').value;
        
        this.filteredPitchers = this.pitchers.filter(pitcher => {
            const tierMatch = !tierFilter || pitcher.tier === tierFilter;
            const teamMatch = !teamFilter || pitcher.team === teamFilter;
            
            return tierMatch && teamMatch;
        });
        
        this.updateTable();
        this.updateSummaryStats();
        
        console.log(`🔍 Filtered to ${this.filteredPitchers.length} pitchers`);
    }
    
    searchPitchers(query) {
        if (!query.trim()) {
            this.applyFilters();
            return;
        }
        
        const searchTerm = query.toLowerCase();
        this.filteredPitchers = this.pitchers.filter(pitcher => {
            return pitcher.pitcher_name.toLowerCase().includes(searchTerm) ||
                   pitcher.team.toLowerCase().includes(searchTerm);
        });
        
        this.updateTable();
        console.log(`🔍 Search results: ${this.filteredPitchers.length} pitchers`);
    }
    
    sortPitchers(field) {
        // Determine sort direction
        if (this.currentSort.field === field) {
            this.currentSort.direction = this.currentSort.direction === 'asc' ? 'desc' : 'asc';
        } else {
            this.currentSort.field = field;
            // Default sort directions for different fields
            this.currentSort.direction = ['whip', 'fip', 'siera', 'xera', 'xfip', 'xwoba', 'xba', 'xslg'].includes(field) ? 'asc' : 'desc';
        }
        
        // Sort the filtered pitchers
        this.filteredPitchers.sort((a, b) => {
            let aVal = a[field];
            let bVal = b[field];
            
            // Handle null values
            if (aVal === null && bVal === null) return 0;
            if (aVal === null) return 1;
            if (bVal === null) return -1;
            
            // Handle string vs number comparison
            if (typeof aVal === 'string') aVal = aVal.toLowerCase();
            if (typeof bVal === 'string') bVal = bVal.toLowerCase();
            
            if (this.currentSort.direction === 'asc') {
                return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
            } else {
                return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
            }
        });
        
        this.updateTable();
        this.updateSortIndicators();
        
        console.log(`📊 Sorted by ${field} (${this.currentSort.direction})`);
    }
    
    updateSortIndicators() {
        // Reset all sort icons
        document.querySelectorAll('.sort-icon').forEach(icon => {
            icon.className = 'fas fa-sort sort-icon';
            icon.style.opacity = '0.5';
        });
        
        // Update active sort icon
        const activeHeader = document.querySelector(`th[data-sort="${this.currentSort.field}"] .sort-icon`);
        if (activeHeader) {
            activeHeader.className = this.currentSort.direction === 'asc' ? 
                'fas fa-sort-up sort-icon' : 'fas fa-sort-down sort-icon';
            activeHeader.style.opacity = '1';
        }
    }
    
    updateTable() {
        const tbody = document.querySelector('#pitcher-table tbody');
        
        // Hide all rows first
        document.querySelectorAll('#pitcher-table tbody tr').forEach(row => {
            row.classList.add('hidden');
        });
        
        // Show and reorder filtered rows
        this.filteredPitchers.forEach((pitcher, index) => {
            pitcher.element.classList.remove('hidden');
            tbody.appendChild(pitcher.element);
            
            // Update rank display
            const rankCell = pitcher.element.querySelector('td:first-child');
            rankCell.textContent = index + 1;
        });
    }
    
    updateSummaryStats() {
        // This would update summary cards based on filtered data
        // Implementation depends on your specific requirements
        console.log('📊 Summary stats updated');
    }
    
    exportData() {
        // Trigger CSV export
        window.location.href = '/export/csv';
    }
    
    // Utility method to format numbers
    formatNumber(value, decimals = 2) {
        if (value === null || value === undefined) return 'N/A';
        return parseFloat(value).toFixed(decimals);
    }
    
    // Method to highlight top performers
    highlightTopPerformers() {
        // Add visual emphasis to top 5 performers
        this.filteredPitchers.slice(0, 5).forEach((pitcher, index) => {
            pitcher.element.classList.add(`top-${index + 1}`);
        });
    }
    
    // Method to add tooltips for metrics
    addTooltips() {
        const tooltips = {
            'WHIP': 'Walks + Hits per Innings Pitched - Lower is better',
            'FIP': 'Fielding Independent Pitching - Lower is better',
            'SIERA': 'Skill-Interactive ERA - Lower is better',
            'CSW%': 'Called Strike + Whiff % - Higher is better',
            'xERA': 'Expected ERA - Lower is better',
            'xFIP': 'Expected FIP - Lower is better',
            'xwOBA': 'Expected Weighted On-Base Average - Lower is better',
            'xBA': 'Expected Batting Average - Lower is better',
            'xSLG': 'Expected Slugging Percentage - Lower is better',
            'Stuff+': 'Pitch Quality Rating - Higher is better (100 = average)'
        };
        
        Object.entries(tooltips).forEach(([metric, description]) => {
            const headers = document.querySelectorAll(`th[data-sort*="${metric.toLowerCase()}"]`);
            headers.forEach(header => {
                header.title = description;
            });
        });
    }
}

// Analytics and Performance Tracking
class DashboardAnalytics {
    constructor() {
        this.startTime = Date.now();
        this.interactions = [];
    }
    
    track(event, data = {}) {
        this.interactions.push({
            event,
            timestamp: Date.now(),
            data
        });
        
        console.log(`📈 Analytics: ${event}`, data);
    }
    
    getSessionStats() {
        const sessionTime = Date.now() - this.startTime;
        return {
            sessionTime: Math.round(sessionTime / 1000),
            interactions: this.interactions.length,
            uniqueEvents: new Set(this.interactions.map(i => i.event)).size
        };
    }
}

// Responsive utilities
class ResponsiveUtils {
    static isMobile() {
        return window.innerWidth <= 768;
    }
    
    static isTablet() {
        return window.innerWidth > 768 && window.innerWidth <= 1024;
    }
    
    static isDesktop() {
        return window.innerWidth > 1024;
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // Initialize main dashboard
    const dashboard = new PitcherDashboard();
    
    // Initialize analytics
    const analytics = new DashboardAnalytics();
    analytics.track('dashboard_loaded');
    
    // Add tooltips
    dashboard.addTooltips();
    
    // Handle responsive changes
    let resizeTimeout;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => {
            analytics.track('viewport_changed', {
                width: window.innerWidth,
                height: window.innerHeight
            });
        }, 250);
    });
    
    // Add keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        // Ctrl/Cmd + F to focus search
        if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
            e.preventDefault();
            const searchInput = document.getElementById('pitcher-search');
            if (searchInput) {
                searchInput.focus();
                analytics.track('keyboard_search_focus');
            }
        }
        
        // Escape to clear search
        if (e.key === 'Escape') {
            const searchInput = document.getElementById('pitcher-search');
            if (searchInput && searchInput === document.activeElement) {
                searchInput.value = '';
                dashboard.applyFilters();
                analytics.track('search_cleared');
            }
        }
    });
    
    // Performance monitoring
    if ('performance' in window) {
        window.addEventListener('load', () => {
            const loadTime = performance.now();
            analytics.track('page_load_complete', {
                loadTime: Math.round(loadTime)
            });
            
            console.log(`⚡ Dashboard loaded in ${Math.round(loadTime)}ms`);
        });
    }
    
    // Export analytics on page unload (for debugging)
    window.addEventListener('beforeunload', () => {
        const stats = analytics.getSessionStats();
        console.log('📊 Session Summary:', stats);
    });
    
    // Make dashboard globally accessible for debugging
    window.pitcherDashboard = dashboard;
    window.dashboardAnalytics = analytics;
    
    console.log('✅ Dashboard fully initialized');
});