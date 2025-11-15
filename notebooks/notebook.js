/**
 * Change page for a table
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table
 * @param {number} direction - Direction to change page (-1 for previous, 1 for next)
 */// Tabbed Tables JavaScript Functions

// Global storage for table instances
window.tableInstances = window.tableInstances || {};

/**
 * Initialize a new table instance
 * @param {string} uid - Unique identifier for this table instance
 * @param {number} tableCount - Number of tables in this instance
 * @param {number} defaultPageSize - Default page size (always 10)
 * @param {boolean} hasSearch - Whether search is enabled
 */
function initializeTableInstance(uid, tableCount, defaultPageSize, hasSearch) {
    // Initialize data structure for this instance
    window.tableInstances[uid] = {
        tables: {},
        hasSearch: hasSearch
    };
    
    // Initialize each table in this instance
    for (let i = 0; i < tableCount; i++) {
        initializeTable(uid, i, 10); // Always use 10 as default
    }
    
    console.log(`✓ Table instance ${uid} initialized with ${tableCount} tables`);
}

/**
 * Initialize a single table within an instance
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table within the instance
 * @param {number} defaultPageSize - Default page size (always 10)
 */
function initializeTable(uid, tableIndex, defaultPageSize) {
    const table = document.getElementById(`table-${uid}-${tableIndex}`);
    
    if (!table) {
        console.error(`Table table-${uid}-${tableIndex} not found`);
        return;
    }
    
    const tbody = table.getElementsByTagName('tbody')[0];
    const rows = tbody ? Array.from(tbody.getElementsByTagName('tr')) : [];
    
    // Store table data
    window.tableInstances[uid].tables[tableIndex] = {
        pageSize: 10, // Always default to 10
        currentPage: 1,
        totalRows: rows.length,
        allRows: rows,
        filteredRows: rows.slice() // Copy of all rows
    };
    
    // Initial display
    updateTable(uid, tableIndex);
}

/**
 * Switch between tabs
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} activeIndex - Index of the tab to activate
 */
function switchTab(uid, activeIndex) {
    const container = document.getElementById(`tabs-${uid}`);
    if (!container) return;
    
    // Get all tabs and panes in this instance
    const tabs = container.querySelectorAll('.tab-btn');
    const panes = container.querySelectorAll('.tab-pane');
    
    // Update tab buttons
    tabs.forEach((tab, index) => {
        if (index === activeIndex) {
            tab.classList.add('active');
        } else {
            tab.classList.remove('active');
        }
    });
    
    // Update panes
    panes.forEach((pane, index) => {
        if (index === activeIndex) {
            pane.style.display = 'block';
        } else {
            pane.style.display = 'none';
        }
    });
}

/**
 * Update table display based on current filters and pagination
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table to update
 */
function updateTable(uid, tableIndex) {
    const instance = window.tableInstances[uid];
    if (!instance || !instance.tables[tableIndex]) {
        console.error(`Table instance ${uid}-${tableIndex} not found`);
        return;
    }
    
    const tableData = instance.tables[tableIndex];
    const pageSizeSelect = document.getElementById(`pagesize-${uid}-${tableIndex}`);
    
    // Update page size
    if (pageSizeSelect) {
        tableData.pageSize = parseInt(pageSizeSelect.value);
    }
    
    // Apply search filter
    let searchText = '';
    if (instance.hasSearch) {
        const searchBox = document.getElementById(`search-${uid}-${tableIndex}`);
        if (searchBox) {
            searchText = searchBox.value.toLowerCase().trim();
        }
    }
    
    // Apply search filter
    applySearchFilter(uid, tableIndex, searchText);
    
    // Reset to page 1 when search or page size changes
    tableData.currentPage = 1;
    
    // Display current page
    displayPage(uid, tableIndex);
}

/**
 * Display the current page of a table
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table to display
 */
function displayPage(uid, tableIndex) {
    const instance = window.tableInstances[uid];
    if (!instance || !instance.tables[tableIndex]) return;
    
    const tableData = instance.tables[tableIndex];
    const startIndex = (tableData.currentPage - 1) * tableData.pageSize;
    const endIndex = Math.min(startIndex + tableData.pageSize, tableData.filteredRows.length);
    
    // Hide all rows first
    tableData.allRows.forEach(row => {
        row.classList.remove('visible');
    });
    
    // Show only current page rows
    if (tableData.filteredRows.length > 0) {
        for (let i = startIndex; i < endIndex; i++) {
            if (tableData.filteredRows[i]) {
                tableData.filteredRows[i].classList.add('visible');
            }
        }
    }
    
    // Update pagination info
    updatePaginationInfo(uid, tableIndex);
}

/**
 * Sort table by column (simplified approach matching your style)
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table
 * @param {HTMLElement} headerElement - The clicked header element
 */
async function sortTable(uid, tableIndex, headerElement) {
    const instance = window.tableInstances[uid];
    if (!instance || !instance.tables[tableIndex]) return;
    
    const table = document.getElementById(`table-${uid}-${tableIndex}`);
    if (!table) return;
    
    // Clear sorting classes from all headers except current
    const allHeaders = table.querySelectorAll('thead th.sortable');
    allHeaders.forEach(header => {
        if (header !== headerElement) {
            header.classList.remove("sorting_asc", "sorting_desc");
            header.classList.add("sorting");
        }
    });
    
    // Determine sort direction
    let ascending = false;
    if (headerElement.classList.contains("sorting")) {
        headerElement.classList.remove("sorting");
        headerElement.classList.add("sorting_desc");
        ascending = false;
    } else if (headerElement.classList.contains("sorting_desc")) {
        headerElement.classList.remove("sorting_desc");
        headerElement.classList.add("sorting_asc");
        ascending = true;
    } else {
        headerElement.classList.remove("sorting_asc");
        headerElement.classList.add("sorting");
        // Reset to original order - just update display without sorting
        updateTable(uid, tableIndex);
        return;
    }
    
    // Get column index
    const columnIndex = Array.from(headerElement.parentNode.children).indexOf(headerElement);
    
    // Helper functions for sorting
    const getCellValue = (tr, idx) => {
        const cell = tr.children[idx];
        return cell ? (cell.innerText || cell.textContent || '').replace(/,/g, '').replace(/\s/g, '') : '';
    };
    
    const comparer = (idx, asc) => (a, b) => {
        const v1 = getCellValue(asc ? a : b, idx);
        const v2 = getCellValue(asc ? b : a, idx);
        
        // Check if both values are numbers
        const num1 = parseFloat(v1);
        const num2 = parseFloat(v2);
        
        if (v1 !== "" && v2 !== "" && !isNaN(num1) && !isNaN(num2)) {
            return num1 - num2;
        } else {
            return v1.toString().localeCompare(v2);
        }
    };
    
    // Get all data rows (skip header)
    const tbody = table.querySelector('tbody');
    if (!tbody) return;
    
    const rows = Array.from(tbody.querySelectorAll('tr'));
    
    // Sort the rows
    const sortedRows = rows.sort(comparer(columnIndex, ascending));
    
    // Remove all rows from tbody
    rows.forEach(row => row.remove());
    
    // Append sorted rows back to tbody
    sortedRows.forEach(row => tbody.appendChild(row));
    
    // Update the table data to reflect new order
    const tableData = instance.tables[tableIndex];
    tableData.allRows = sortedRows;
    
    // Reapply current filters and pagination
    updateTable(uid, tableIndex);
}

/**
 * Apply search filter to table data
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table
 * @param {string} searchText - Search text to filter by
 */
function applySearchFilter(uid, tableIndex, searchText) {
    const instance = window.tableInstances[uid];
    if (!instance || !instance.tables[tableIndex]) return;
    
    const tableData = instance.tables[tableIndex];
    
    if (searchText) {
        tableData.filteredRows = tableData.allRows.filter(row => {
            const cells = row.getElementsByTagName('td');
            for (let i = 0; i < cells.length; i++) {
                if (cells[i].textContent.toLowerCase().includes(searchText)) {
                    return true;
                }
            }
            return false;
        });
    } else {
        tableData.filteredRows = tableData.allRows.slice();
    }
    
    // Reset to page 1 and display
    tableData.currentPage = 1;
    displayPage(uid, tableIndex);
}
function changePage(uid, tableIndex, direction) {
    const instance = window.tableInstances[uid];
    if (!instance || !instance.tables[tableIndex]) return;
    
    const tableData = instance.tables[tableIndex];
    const totalPages = Math.ceil(tableData.filteredRows.length / tableData.pageSize);
    
    // Calculate new page
    const newPage = tableData.currentPage + direction;
    
    // Ensure page is within bounds
    if (newPage >= 1 && newPage <= totalPages) {
        tableData.currentPage = newPage;
        displayPage(uid, tableIndex);
    }
}

/**
 * Update pagination information and controls
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table
 */
function updatePaginationInfo(uid, tableIndex) {
    const instance = window.tableInstances[uid];
    if (!instance || !instance.tables[tableIndex]) return;
    
    const tableData = instance.tables[tableIndex];
    const totalPages = Math.ceil(tableData.filteredRows.length / tableData.pageSize);
    const startRow = (tableData.currentPage - 1) * tableData.pageSize + 1;
    const endRow = Math.min(tableData.currentPage * tableData.pageSize, tableData.filteredRows.length);
    
    // Update page info text
    const pageInfo = document.getElementById(`pageinfo-${uid}-${tableIndex}`);
    if (pageInfo) {
        if (tableData.filteredRows.length === 0) {
            pageInfo.textContent = 'No results found';
        } else {
            pageInfo.textContent = `${startRow}-${endRow} of ${tableData.filteredRows.length}`;
        }
    }
    
    // Update navigation button states
    const prevBtn = document.getElementById(`prev-${uid}-${tableIndex}`);
    const nextBtn = document.getElementById(`next-${uid}-${tableIndex}`);
    
    if (prevBtn) {
        prevBtn.disabled = tableData.currentPage === 1;
    }
    
    if (nextBtn) {
        nextBtn.disabled = tableData.currentPage === totalPages || totalPages === 0;
    }
}

/**
 * Go to a specific page
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table
 * @param {number} pageNumber - Page number to go to
 */
function goToPage(uid, tableIndex, pageNumber) {
    const instance = window.tableInstances[uid];
    if (!instance || !instance.tables[tableIndex]) return;
    
    const tableData = instance.tables[tableIndex];
    const totalPages = Math.ceil(tableData.filteredRows.length / tableData.pageSize);
    
    if (pageNumber >= 1 && pageNumber <= totalPages) {
        tableData.currentPage = pageNumber;
        displayPage(uid, tableIndex);
    }
}

/**
 * Clear search for a specific table
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table
 */
function clearSearch(uid, tableIndex) {
    const searchBox = document.getElementById(`search-${uid}-${tableIndex}`);
    if (searchBox) {
        searchBox.value = '';
        updateTable(uid, tableIndex);
    }
}

/**
 * Get current table statistics
 * @param {string} uid - Unique identifier for the table instance
 * @param {number} tableIndex - Index of the table
 * @returns {Object} Statistics object
 */
function getTableStats(uid, tableIndex) {
    const instance = window.tableInstances[uid];
    if (!instance || !instance.tables[tableIndex]) return null;
    
    const tableData = instance.tables[tableIndex];
    const totalPages = Math.ceil(tableData.filteredRows.length / tableData.pageSize);
    
    return {
        totalRows: tableData.totalRows,
        filteredRows: tableData.filteredRows.length,
        currentPage: tableData.currentPage,
        totalPages: totalPages,
        pageSize: tableData.pageSize
    };
}

// Export functions for module systems (if needed)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        initializeTableInstance,
        initializeTable,
        switchTab,
        updateTable,
        displayPage,
        changePage,
        updatePaginationInfo,
        goToPage,
        clearSearch,
        getTableStats,
        sortTable,
        applySearchFilter,
        loadFigure
    };
}

/**
 * Generic function to load and display Plotly figures from JSON files
 * @param {string} filename - Name of the JSON file to load
 * @param {string} label - Label for display in loading messages
 * @param {string} containerId - ID of the container element to render the plot in
 * @param {string} basePath - Base path where the JSON files are located
 * @param {string} cacheKey - Unique key for caching (typically uid)
 */
async function loadFigure(filename, label, basePath, cacheKey) {
    const container = document.getElementById(`figure-display-${cacheKey}`);
    
    const cacheVariable = `figuresCache_${cacheKey}`;
    if (!window[cacheVariable]) {
        window[cacheVariable] = {};
    }
    
    // Show loading state
    container.innerHTML = `<div class="loading-message">Loading ${label}...</div>`;
    
    // Check if already cached
    if (window[cacheVariable][filename]) {
        const plotId = `plot-${cacheKey}`;
        container.innerHTML = `<div id="${plotId}"></div>`;
        const plotDiv = document.getElementById(plotId);
        Plotly.newPlot(plotDiv, window[cacheVariable][filename].data, window[cacheVariable][filename].layout, {responsive: true});
        return;
    }
    const filepath = `${basePath}/${filename}`
    
    try {
        // Fetch JSON data
        const response = await fetch(filepath);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const figJson = await response.json();
        
        // Cache the figure data
        window[cacheVariable][filename] = figJson;
        
        // Create plot container and render
        const plotId = `plot-${cacheKey}`;
        container.innerHTML = `<div id="${plotId}" style="width: 100%; height: 600px;"></div>`;
        const plotDiv = document.getElementById(plotId);
        
        await Plotly.newPlot(plotDiv, figJson.data, figJson.layout, {responsive: true});
        console.log('Figure loaded and cached:', filename);
        
    } catch (error) {
        console.error('Failed to load figure:', filename, error);
        container.innerHTML = `<div style="text-align: center; color: #d32f2f; font-size: 16px; padding: 50px;">Failed to load figure: ${filepath}<br><small>${error.message}</small></div>`;
    }
}

// Debug helper
window.debugTables = function() {
    console.log('Active table instances:', window.tableInstances);
};
