/**
 * Enhanced MLB Betting Dashboard JavaScript
 * Handles interactivity and real-time updates
 */

class EnhancedDashboard {
    constructor() {
        this.refreshInterval = 300000; // 5 minutes
        this.refreshTimer = null;
        this.lastKnownDate = null;
        this.pageLoadTime = new Date();
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.startAutoRefresh();
        this.loadInitialData();
    }

    setupEventListeners() {
        // Refresh button
        const refreshBtns = document.querySelectorAll('.refresh-btn');
        refreshBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                this.refreshData();
            });
        });

        // Navigation
        const navItems = document.querySelectorAll('.nav-item');
        navItems.forEach(item => {
            item.addEventListener('click', (e) => {
                // Let the browser handle navigation, but update active state
                navItems.forEach(nav => nav.classList.remove('active'));
                item.classList.add('active');
            });
        });

        // Table interactions
        this.setupTableInteractions();
        
        // Matchup card interactions
        this.setupMatchupInteractions();
    }

    setupTableInteractions() {
        const tableRows = document.querySelectorAll('.games-table tbody tr');
        tableRows.forEach(row => {
            row.addEventListener('click', () => {
                // Highlight selected row
                tableRows.forEach(r => r.classList.remove('selected'));
                row.classList.add('selected');
                
                // Could add detail view here
                this.showGameDetails(row);
            });
        });
    }

    setupMatchupInteractions() {
        const matchupCards = document.querySelectorAll('.matchup-card');
        matchupCards.forEach(card => {
            card.addEventListener('click', () => {
                // Add click effect
                card.style.transform = 'scale(0.98)';
                setTimeout(() => {
                    card.style.transform = '';
                }, 150);
                
                // Could show detailed matchup analysis
                this.showMatchupDetails(card);
            });
        });
    }

    showGameDetails(row) {
        // Extract game data from row
        const matchupCell = row.querySelector('.matchup-cell');
        const timeCell = row.cells[1];
        const oddsCell = row.querySelector('.odds-cell');
        
        if (matchupCell && timeCell && oddsCell) {
            const teams = matchupCell.querySelector('.teams').textContent;
            const pitchers = matchupCell.querySelector('.pitchers').textContent;
            const time = timeCell.textContent;
            const odds = oddsCell.textContent.replace(/\s+/g, ' ');
            
            // Show details in console for now (could be modal)
            console.log('Game Details:', {
                teams,
                pitchers,
                time,
                odds
            });
        }
    }

    showMatchupDetails(card) {
        // Extract matchup data from card
        const awayTeam = card.querySelector('.away-team strong')?.textContent;
        const homeTeam = card.querySelector('.home-team strong')?.textContent;
        const confidence = card.querySelector('.confidence-badge')?.textContent;
        const scoreDiff = card.querySelector('.score-diff')?.textContent;
        
        console.log('Matchup Details:', {
            awayTeam,
            homeTeam,
            confidence,
            scoreDiff
        });
    }

    async loadInitialData() {
        try {
            // Load summary data
            await this.loadSummaryData();
            
            // Store initial date for comparison
            const response = await fetch('/api/summary');
            const data = await response.json();
            this.lastKnownDate = data.current_date;
            
            // Update last refresh time
            this.updateLastRefreshTime();
            
        } catch (error) {
            console.error('Error loading initial data:', error);
            this.showError('Failed to load dashboard data');
        }
    }

    async loadSummaryData() {
        try {
            const response = await fetch('/api/summary');
            if (!response.ok) throw new Error('Failed to fetch summary');
            
            const data = await response.json();
            this.updateSummaryDisplay(data);
            
        } catch (error) {
            console.error('Error loading summary data:', error);
        }
    }

    updateSummaryDisplay(data) {
        // Update summary cards with fresh data
        if (data.pitcher_summary) {
            const pitcherCount = document.querySelector('.pitchers-card h3');
            if (pitcherCount && data.pitcher_summary.total_pitchers) {
                pitcherCount.textContent = data.pitcher_summary.total_pitchers;
            }
            
            const pitcherAvg = document.querySelector('.pitchers-card small');
            if (pitcherAvg && data.pitcher_summary.average_score) {
                pitcherAvg.textContent = `Avg Score: ${data.pitcher_summary.average_score.toFixed(1)}`;
            }
        }
        
        if (data.hitter_summary) {
            const hitterCount = document.querySelector('.hitters-card h3');
            if (hitterCount && data.hitter_summary.total_hitters) {
                hitterCount.textContent = data.hitter_summary.total_hitters;
            }
            
            const hitterAvg = document.querySelector('.hitters-card small');
            if (hitterAvg && data.hitter_summary.average_score) {
                hitterAvg.textContent = `Avg Score: ${data.hitter_summary.average_score.toFixed(1)}`;
            }
        }
        
        if (data.games_count !== undefined) {
            const gamesCount = document.querySelector('.games-card h3');
            if (gamesCount) {
                gamesCount.textContent = data.games_count;
            }
        }
    }

    async refreshData() {
        const refreshBtns = document.querySelectorAll('.refresh-btn');
        
        // Show loading state
        refreshBtns.forEach(btn => {
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Loading...';
            btn.disabled = true;
        });
        
        try {
            // Check if we need to reload for date change
            const response = await fetch('/api/summary');
            const data = await response.json();
            const currentDate = data.current_date;
            
            // If date changed or no data cached, reload the entire page
            if (!this.lastKnownDate || this.lastKnownDate !== currentDate || this.isNewDay()) {
                console.log('Date changed or new day detected, reloading page...');
                location.reload();
                return;
            }
            
            // Otherwise just refresh summary data
            await this.loadSummaryData();
            
            // Update refresh time
            this.updateLastRefreshTime();
            
            // Show success feedback
            this.showSuccess('Data refreshed successfully');
            
        } catch (error) {
            console.error('Error refreshing data:', error);
            this.showError('Failed to refresh data');
        } finally {
            // Reset buttons
            setTimeout(() => {
                refreshBtns.forEach(btn => {
                    btn.innerHTML = '<i class="fas fa-sync"></i> Refresh';
                    btn.disabled = false;
                });
            }, 1000);
        }
    }

    startAutoRefresh() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
        }
        
        this.refreshTimer = setInterval(() => {
            this.refreshData();
        }, this.refreshInterval);
        
        console.log(`Auto-refresh enabled (${this.refreshInterval / 1000}s interval)`);
    }

    stopAutoRefresh() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
            this.refreshTimer = null;
        }
    }

    updateLastRefreshTime() {
        const now = new Date();
        const timeString = now.toLocaleTimeString('en-US', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
        
        // Update any "last updated" displays
        const lastUpdated = document.querySelector('.last-updated');
        if (lastUpdated) {
            lastUpdated.textContent = `Last updated: ${timeString}`;
        }
    }

    isNewDay() {
        const now = new Date();
        const loadDate = this.pageLoadTime;
        
        // Check if we've crossed into a new day (Eastern Time)
        const nowEastern = new Date(now.toLocaleString("en-US", {timeZone: "America/New_York"}));
        const loadEastern = new Date(loadDate.toLocaleString("en-US", {timeZone: "America/New_York"}));
        
        return nowEastern.getDate() !== loadEastern.getDate() || 
               nowEastern.getMonth() !== loadEastern.getMonth() ||
               nowEastern.getFullYear() !== loadEastern.getFullYear();
    }

    showSuccess(message) {
        this.showNotification(message, 'success');
    }

    showError(message) {
        this.showNotification(message, 'error');
    }

    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <div class="notification-content">
                <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i>
                <span>${message}</span>
            </div>
            <button class="notification-close">
                <i class="fas fa-times"></i>
            </button>
        `;

        // Add styles
        Object.assign(notification.style, {
            position: 'fixed',
            top: '20px',
            right: '20px',
            background: type === 'success' ? '#10b981' : type === 'error' ? '#ef4444' : '#3b82f6',
            color: 'white',
            padding: '16px 20px',
            borderRadius: '8px',
            boxShadow: '0 10px 25px rgba(0, 0, 0, 0.2)',
            zIndex: '1000',
            display: 'flex',
            alignItems: 'center',
            gap: '12px',
            maxWidth: '400px',
            transform: 'translateX(100%)',
            transition: 'transform 0.3s ease'
        });

        // Add to page
        document.body.appendChild(notification);

        // Animate in
        setTimeout(() => {
            notification.style.transform = 'translateX(0)';
        }, 100);

        // Auto remove after 5 seconds
        const autoRemove = setTimeout(() => {
            this.removeNotification(notification);
        }, 5000);

        // Manual close button
        const closeBtn = notification.querySelector('.notification-close');
        closeBtn.style.cssText = 'background: none; border: none; color: inherit; cursor: pointer; padding: 4px;';
        closeBtn.addEventListener('click', () => {
            clearTimeout(autoRemove);
            this.removeNotification(notification);
        });
    }

    removeNotification(notification) {
        notification.style.transform = 'translateX(100%)';
        setTimeout(() => {
            if (notification.parentNode) {
                notification.parentNode.removeChild(notification);
            }
        }, 300);
    }

    // Utility methods
    formatCurrency(value, prefix = '$') {
        if (value === null || value === undefined || value === 'N/A') return 'N/A';
        return `${prefix}${Math.abs(value).toFixed(0)}`;
    }

    formatOdds(odds) {
        if (odds === null || odds === undefined || odds === 'N/A') return 'N/A';
        const num = parseInt(odds);
        return num > 0 ? `+${num}` : `${num}`;
    }

    formatPercentage(value, decimals = 1) {
        if (value === null || value === undefined || value === 'N/A') return 'N/A';
        return `${(value * 100).toFixed(decimals)}%`;
    }

    // Export functionality
    exportData(type) {
        const links = {
            games: '/export/games',
            pitchers: '/export/pitchers',
            hitters: '/export/hitters'
        };
        
        const url = links[type];
        if (url) {
            window.open(url, '_blank');
            this.showSuccess(`Exporting ${type} data...`);
        } else {
            this.showError(`Export type "${type}" not supported`);
        }
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new EnhancedDashboard();
});

// Handle page visibility changes (pause/resume auto-refresh)
document.addEventListener('visibilitychange', () => {
    if (window.dashboard) {
        if (document.hidden) {
            window.dashboard.stopAutoRefresh();
        } else {
            window.dashboard.startAutoRefresh();
            window.dashboard.refreshData();
        }
    }
});

// Handle window beforeunload
window.addEventListener('beforeunload', () => {
    if (window.dashboard) {
        window.dashboard.stopAutoRefresh();
    }
});

// Expose utility functions globally
window.dashboardUtils = {
    exportData: (type) => window.dashboard?.exportData(type),
    refreshData: () => window.dashboard?.refreshData(),
    formatOdds: (odds) => window.dashboard?.formatOdds(odds),
    formatCurrency: (value, prefix) => window.dashboard?.formatCurrency(value, prefix)
};