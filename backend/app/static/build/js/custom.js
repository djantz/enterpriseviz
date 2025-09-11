$('.collapse-link').on('click', function () {
    var $BOX_PANEL = $(this).closest('.x_panel');
    var $ICON = $(this).find('calcite-icon');
    var $BOX_CONTENT = $BOX_PANEL.find('.x_content');

    $BOX_CONTENT.slideToggle(200, function () {
        if ($BOX_CONTENT.is(':visible')) {
            $BOX_PANEL.addClass('panel-height-auto');
        } else {
            $BOX_PANEL.removeClass('panel-height-auto');
        }
    });

    var currentIcon = $ICON.attr('icon');
    var newIcon = (currentIcon === 'chevron-up') ? 'chevron-down' : 'chevron-up';
    $ICON.attr('icon', newIcon);
});


$('.close-link').click(function () {
    var $BOX_PANEL = $(this).closest('.x_panel');
    $BOX_PANEL.remove();
});


function init_DataTables() {
    // Ensure moment.js sorting plugin is loaded
    if ($.fn.dataTable.moment === undefined) {
        return;
    }

    // Register Moment.js formats for sorting
    $.fn.dataTable.moment('lll');
    $.fn.dataTable.moment('MMM D, YYYY');
    $.fn.dataTable.moment('MM/DD/YYYY');

    // Check if any table exists on the page before initializing
    if ($("table[id^='datatable']").length === 0) {
        return;
    }

    function initDataTable(selector, filterSelector, columnContains, usageTarget) {
        return $(selector).DataTable({
            layout: {
                topStart: {
                    buttons: [
                        {
                            extend: "copyHtml5",
                            tag: 'calcite-button',
                            attr: {scale: "s", kind: "inverse", "icon-start": "copy-to-clipboard"}
                        },
                        {
                            extend: "csvHtml5",
                            tag: 'calcite-button',
                            attr: {scale: "s", kind: "inverse", "icon-start": "file-csv"}
                        },
                        {
                            extend: "excelHtml5",
                            tag: 'calcite-button',
                            attr: {scale: "s", kind: "inverse", "icon-start": "file-excel"}
                        },
                        {
                            extend: "pdfHtml5",
                            tag: 'calcite-button',
                            attr: {scale: "s", kind: "inverse", "icon-start": "file-pdf"}
                        }
                    ]
                },
                bottomEnd: {
                    paging: {
                        firstLast: false
                    }
                }
            },
            language: {
                paginate: {
                    next: '<calcite-icon icon="chevron-right" preload="true" scale="s"></calcite-icon>',
                    previous: '<calcite-icon icon="chevron-left" preload="true" scale="s"></calcite-icon>',
                    first: 'First',
                    last: 'Last'
                },
                info: "Displaying records _START_ to _END_ of _TOTAL_ records",
                emptyTable: "No records available.",
                infoFiltered: "(filtered from _MAX_ records)",
                infoEmpty: "Displaying 0 to 0 of 0 records"
            },
            autoWidth: false,
            order: [[1, "asc"]],
            responsive: true,
            columnDefs: [
                {targets: 'details', width: '40px', orderable: false},
                {targets: 'instance', visible: false, searchable: true},
                {targets: 'url', visible: false, searchable: true},
                {targets: 'trend', width: '50px'}
            ],
            orderCellsTop: true,
            initComplete: function () {
                this.api().columns(`:contains(${columnContains})`).every(function (d) {
                    var column = this;
                    var $select = $('<calcite-combobox placeholder="Instance" scale="s" selection-mode="single" placeholder-icon="filter"></calcite-combobox>');
                    column.data().unique().sort().each(function (d, j) {
                        var val = $('<div/>').html(d).text();
                        $select.append(`<calcite-combobox-item value="${val}" heading="${val}"></calcite-combobox-item>`);
                    });
                    $select.appendTo(filterSelector);
                    $select.on('calciteComboboxChange', function () {
                        var val = $.fn.dataTable.util.escapeRegex($(this).val());
                        column.search(val ? '^' + val + '$' : '', true, false).draw();
                    });
                });
            }
        });
    }

    // Initialize tables only if they exist
    ["maps", "services", "layers", "apps", "users"].forEach(table => {
        if ($(`#datatable-${table}`).length) {
            let tableInstance = initDataTable(`#datatable-${table}`, `#datatable-${table}-filter`, "Instance", false);
            tableInstance.on('draw', () => htmx.process(document.body));
        }
    });
}

function init_Charts() {
    const ctx = document.getElementById('line-chart');

    if (!ctx) return; // Exit if no chart container is found

    $(function () {
        const borderColor = getComputedStyle(document.body).getPropertyValue('--calcite-color-border-3');
        const fontColor = getComputedStyle(document.body).getPropertyValue('--calcite-color-text-2');
        const chartDataElement = document.getElementById('chartdata');

        if (!chartDataElement) {
            return;
        }

        const data = JSON.parse(chartDataElement.textContent);

        // Initialize color function
        const getNextColor = customColors();

        // Assign colors dynamically
        data.datasets.forEach(dataset => {
            dataset.borderColor = getNextColor();
            dataset.backgroundColor = dataset.borderColor + '90'; // Add transparency
        });

        const options = {
            elements: {
                line: {
                    tension: 0.2
                }
            },
            scales: {
                y: {
                    title: {
                        color: fontColor,
                        display: true,
                        text: 'RequestCount'
                    },
                    grid: {
                        color: borderColor
                    }
                },
                x: {
                    type: 'time',
                    time: {
                        unit: 'day',
                        displayFormats: {
                            day: 'MM/DD'
                        },
                        tooltipFormat: 'MM/DD/YYYY'
                    },
                    title: {
                        color: fontColor
                    },
                    grid: {
                        color: borderColor
                    }
                }
            },
            interaction: {
                intersect: false,
                mode: 'index',
            },
            plugins: {
                legend: {
                    display: true,
                    labels: {
                        color: fontColor
                    }
                }
            },
            responsive: true,
            aspectRatio: 2.5,
            maintainAspectRatio: false
        };

        const myChart = new Chart(ctx, {
            type: 'line',
            data: data,
            options: options
        });

        // Create dropdown filter
        const $select = $('<calcite-combobox placeholder="Instance" scale="s" selection-mode="single" placeholder-icon="filter"></calcite-combobox>');
        const instances = new Set();

        // Populate unique instances
        for (const dataset of data.datasets) {
            const instanceName = dataset.label.split('/')[0];
            instances.add(instanceName);
        }

        // Append unique instances to dropdown
        for (const instance of instances) {
            $select.append(`<calcite-combobox-item value="${instance}" heading="${instance}"></calcite-combobox-item>`);
        }

        // Ensure the dropdown is empty before appending (prevents duplicates)
        $('#chartjs-filter').empty().append($select);

        // Handle filtering on selection
        $select.on('calciteComboboxChange', function () {
            const selectedValue = this.value; // Gets the selected instance
            const originalData = JSON.parse(chartDataElement.textContent);

            const filteredDatasets = selectedValue
                ? originalData.datasets.filter(dataset => dataset.label.startsWith(selectedValue))
                : originalData.datasets;

            myChart.data.datasets = filteredDatasets;
            myChart.update();
        });
    });
}

// Custom color function to ensure all colors are used before repeating
function getInitialLogVisibleColumns() {
    let initialVisibleColumns = [];
    const visibleColsDataElement = document.getElementById('log_initial_visible_cols_data');
    if (visibleColsDataElement) {
        try {
            initialVisibleColumns = JSON.parse(visibleColsDataElement.textContent);
        } catch (e) {
            console.error("Failed to parse initial visible columns from json_script:", e);
        }
    }
    return initialVisibleColumns;
}

function init_LogFilters(logTableUrl) {
    const applyButton = document.getElementById('apply-filters-and-visibility');
    const resetButton = document.getElementById('reset-filters-and-visibility');

    const columnVisibilityDropdown = document.getElementById('column-visibility-dropdown');
    const levelDropdown = document.getElementById('level-dropdown');
    const timeRangeDropdown = document.getElementById('time-range-dropdown');

    const visibleColsHiddenInput = document.getElementById('visible_cols_hidden_input');
    const levelHiddenInput = document.getElementById('level_hidden_input');
    const timeRangeHiddenInput = document.getElementById('time_range_hidden_input');

    const logForm = document.getElementById('log-form');

    if (!logForm) {
        console.log("Log filter elements not found, skipping init_LogFilters.");
        return;
    }

    // Get initial visible columns using the helper
    const initialVisibleColumns = getInitialLogVisibleColumns();

    async function initializeDropdown(dropdownElement, hiddenInputElement, isMultiSelect, buttonTextElementId, defaultButtonText) {
        if (!dropdownElement) return;
        if (typeof customElements !== 'undefined' && customElements.whenDefined) {
            await Promise.all([
                customElements.whenDefined('calcite-dropdown'),
                customElements.whenDefined('calcite-dropdown-item'),
                customElements.whenDefined('calcite-dropdown-group')
            ]);
        }
        if (dropdownElement.componentOnReady) await dropdownElement.componentOnReady();

        const buttonTextElement = buttonTextElementId ? document.getElementById(buttonTextElementId) : null;
        const selectedItems = Array.from(dropdownElement.selectedItems || []);
        const selectedValues = selectedItems.map(item => item.getAttribute('value')).filter(v => v !== null && v !== undefined && v !== '');

        if (hiddenInputElement) {
            hiddenInputElement.value = selectedValues.join(',');
        }

        if (buttonTextElement) {
            if (selectedValues.length > 0) {
                const selectedTexts = selectedItems.map(item => item.textContent.trim());
                if (isMultiSelect) {
                    buttonTextElement.textContent = selectedTexts.join(', ');
                } else {
                    buttonTextElement.textContent = selectedTexts.length > 0 ? selectedTexts[0] : defaultButtonText;
                }
            } else {
                buttonTextElement.textContent = defaultButtonText;
            }
        }

        dropdownElement.addEventListener('calciteDropdownSelect', () => {
            const currentSelectedItems = Array.from(dropdownElement.selectedItems || []);
            const currentSelectedValues = currentSelectedItems.map(item => item.getAttribute('value')).filter(v => v !== null && v !== undefined && v !== '');

            if (hiddenInputElement) {
                hiddenInputElement.value = currentSelectedValues.join(',');
            }
            if (buttonTextElement) {
                if (currentSelectedValues.length > 0) {
                    const currentSelectedTexts = currentSelectedItems.map(item => item.textContent.trim());
                    if (isMultiSelect) {
                        buttonTextElement.textContent = currentSelectedTexts.join(', ');
                    } else {
                        buttonTextElement.textContent = currentSelectedTexts.length > 0 ? currentSelectedTexts[0] : defaultButtonText;
                    }
                } else {
                    buttonTextElement.textContent = defaultButtonText;
                }
            }
            if (dropdownElement.id === 'column-visibility-dropdown') {
                dropdownElement.setAttribute('data-interacted', 'true');
            }
        });
    }

    async function initializeColumnVisibility(initialCols) {
        if (!columnVisibilityDropdown || !visibleColsHiddenInput) return;
        if (typeof customElements !== 'undefined' && customElements.whenDefined) {
            await Promise.all([
                customElements.whenDefined('calcite-dropdown'),
                customElements.whenDefined('calcite-dropdown-item')
            ]);
        }
        if (columnVisibilityDropdown.componentOnReady) await columnVisibilityDropdown.componentOnReady();

        const items = columnVisibilityDropdown.querySelectorAll('calcite-dropdown-item');
        let currentVisibleForInput = [];

        items.forEach(item => {
            const itemValue = item.getAttribute('value');
            const isSelected = initialCols.includes(itemValue); // Use the passed-in initialCols
            item.selected = isSelected;
            if (isSelected) {
                currentVisibleForInput.push(itemValue);
            }
        });
        visibleColsHiddenInput.value = currentVisibleForInput.join(',');

        columnVisibilityDropdown.addEventListener('calciteDropdownSelect', () => {
            const selectedItems = Array.from(columnVisibilityDropdown.selectedItems || []);
            const selectedValues = selectedItems.map(item => item.getAttribute('value')).filter(v => v !== null && v !== undefined && v !== '');
            visibleColsHiddenInput.value = selectedValues.join(',');
            columnVisibilityDropdown.setAttribute('data-interacted', 'true');
        });
    }

    (async () => {
        await initializeColumnVisibility(initialVisibleColumns);
        await initializeDropdown(levelDropdown, levelHiddenInput, true, 'level-dropdown-button-text', 'Level');
        await initializeDropdown(timeRangeDropdown, timeRangeHiddenInput, false, 'time-range-dropdown-button-text', 'Any Time');
    })();

    function triggerTableUpdate() {
        if (!logForm) return;
        const params = new URLSearchParams(new FormData(logForm));

        // If you need 'visible_cols' as multiple params (e.g. for Django's MultipleChoiceFilter from query params):
        const currentVisibleColsVal = params.get('visible_cols_hidden');
        params.delete('visible_cols_hidden');

        if (currentVisibleColsVal) {
            const visibleColsArray = currentVisibleColsVal.split(',').filter(v => v);
            if (visibleColsArray.length > 0) {
                visibleColsArray.forEach(col => params.append('visible_cols', col));
            } else if (columnVisibilityDropdown && columnVisibilityDropdown.hasAttribute('data-interacted')) {
                params.append('visible_cols', ''); // User deselected all
            }
        } else if (columnVisibilityDropdown && columnVisibilityDropdown.hasAttribute('data-interacted')) {
            params.append('visible_cols', ''); // User deselected all, and hidden input is now empty
        }
        // If not interacted, and hidden input was empty, no 'visible_cols' param will be sent.

        const targetUrlWithParams = `${logTableUrl}?${params.toString()}`;
        htmx.ajax('GET', targetUrlWithParams, {
            target: '#log-table-container',
            swap: 'innerHTML'
        });
    }

    if (applyButton) {
        applyButton.addEventListener('click', triggerTableUpdate);
    }

    if (resetButton) {
        resetButton.addEventListener('click', async function () {
            if (logForm) {
                logForm.querySelectorAll('input[type="text"], calcite-input-text').forEach(input => {
                    if (input.name !== 'visible_cols_hidden' && input.name !== 'level' && input.name !== 'time_range') {
                        if (input.tagName === 'CALCITE-INPUT-TEXT' && input.value) {
                            input.value = '';
                        } else if (input.tagName === 'INPUT') {
                            input.value = '';
                        }
                    }
                });

                const colsToResetTo = getInitialLogVisibleColumns(); // Get fresh initial columns for reset

                if (columnVisibilityDropdown) {
                    if (columnVisibilityDropdown.componentOnReady) await columnVisibilityDropdown.componentOnReady();
                    const allColumnItems = columnVisibilityDropdown.querySelectorAll('calcite-dropdown-item');
                    let resetVisibleColsForInput = [];
                    allColumnItems.forEach(item => {
                        const itemValue = item.getAttribute('value');
                        const isSelected = colsToResetTo.includes(itemValue); // Reset to initial state
                        item.selected = isSelected;
                        if (isSelected) {
                            resetVisibleColsForInput.push(itemValue);
                        }
                    });
                    if (visibleColsHiddenInput) visibleColsHiddenInput.value = resetVisibleColsForInput.join(',');
                    columnVisibilityDropdown.removeAttribute('data-interacted');
                }
                if (levelDropdown) {
                    if (levelDropdown.componentOnReady) await levelDropdown.componentOnReady();
                    Array.from(levelDropdown.selectedItems || []).forEach(item => item.selected = false);
                    if (levelHiddenInput) levelHiddenInput.value = '';
                    const levelButtonText = document.getElementById('level-dropdown-button-text');
                    if (levelButtonText) levelButtonText.textContent = 'Level';
                }
                if (timeRangeDropdown) {
                    if (timeRangeDropdown.componentOnReady) await timeRangeDropdown.componentOnReady();
                    const timeRangeItems = timeRangeDropdown.querySelectorAll('calcite-dropdown-item');
                    let anyTimeSelected = false;
                    timeRangeItems.forEach(item => {
                        if (item.getAttribute('value') === '') { // Default "Any Time"
                            item.selected = true;
                            anyTimeSelected = true;
                        } else {
                            item.selected = false;
                        }
                    });
                    // Fallback logic if "Any Time" (empty value) isn't present or isn't first
                    if (!anyTimeSelected && timeRangeItems.length > 0) {
                        let defaultSelectedItem = timeRangeItems[0]; // Default to first
                        // Prefer selecting an item that was initially selected if possible
                        const initialTimeRangeValue = document.querySelector('#time_range_hidden_input[name="time_range"]')?.defaultValue || '';
                        const initiallySelectedItem = Array.from(timeRangeItems).find(item => item.getAttribute('value') === initialTimeRangeValue);
                        if (initiallySelectedItem) defaultSelectedItem = initiallySelectedItem;

                        defaultSelectedItem.selected = true;
                        if (timeRangeHiddenInput) timeRangeHiddenInput.value = defaultSelectedItem.getAttribute('value');
                        const timeRangeButtonText = document.getElementById('time-range-dropdown-button-text');
                        if (timeRangeButtonText) timeRangeButtonText.textContent = defaultSelectedItem.textContent.trim() || 'Any Time';
                    } else { // "Any Time" was selected or no items
                        if (timeRangeHiddenInput) timeRangeHiddenInput.value = '';
                        const timeRangeButtonText = document.getElementById('time-range-dropdown-button-text');
                        if (timeRangeButtonText) timeRangeButtonText.textContent = 'Any Time';
                    }
                }
            }
            triggerTableUpdate();
        });
    }
}


function customColors() {
    // Esri Calcite colors
    const underTheSea = ["#bf9727", "#607100", "#00734c", "#704489", "#01acca", "#024e76", "#f09100", "#ea311f", "#c6004b", "#7570b3", "#666666", "#333333"];
    const vibrantRainbow = ["#fffb00", "#f5cb11", "#9fd40c", "#46e39c", "#32b8a6", "#7ff2fa", "#ac08cc", "#dd33ff", "#eb7200", "#e8a784", "#bf2e2e", "#6c7000"];
    const sixty = ['#edd317', "#f89927", "#f36f20", "#da4d1e", "#d83020", "#e04ea6", "#8e499b", "#633b9b", "#007ac2", "#00bab5", "#35ac46", "#aad04b", "#7b4f1c"];
    const vibrant = ["#fff766", "#ffb54d", "#ff974d", "#ff824d", "#ff624d", "#ff66c2", "#ea80ff", "#b580ff", "#59d6ff", "#59fffc", "#73ff84", "#d7ff73"];

    let shuffledColors = shuffleArray([...sixty]);
    let colorIndex = 0;

    return function getNextColor() {
        if (colorIndex >= shuffledColors.length) {
            shuffledColors = shuffleArray([...sixty]); // Reshuffle when exhausted
            colorIndex = 0;
        }
        return shuffledColors[colorIndex++];
    };
}

function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

function syncSelected(tableId, inputId) {
    const table = document.getElementById(tableId);
    const hiddenInput = document.getElementById(inputId);
    if (!table || !hiddenInput) return;

    table.addEventListener("calciteTableSelect", () => {
        const selected = table.selectedItems.map(row => row.dataset.id);
        hiddenInput.value = selected.join(",");
    });
}

function initPortalNotification() {
    syncSelected("maps-table", "selected-maps");
    syncSelected("apps-table", "selected-apps");
    const notifyModal = document.getElementById("notify-modal");
    if (notifyModal) {
        setupNotifyModalStepper();
    }
    document.addEventListener('click', function (event) {
        const button = event.target.closest('calcite-button[onclick*="notify-modal"]');
        if (button) {
            setTimeout(() => {
                setupNotifyModalStepper();
            }, 100);
        }
    });
}

function setupNotifyModalStepper() {
    const stepper = document.getElementById("notify-stepper");
    const saveButton = document.getElementById("notify-portal-save");
    const form = document.getElementById("notify-portal-form");

    if (!stepper || !saveButton || !form) return;

    let currentStepIndex = 0;
    const stepperItems = stepper.querySelectorAll("calcite-stepper-item");
    currentStepIndex = Array.from(stepperItems).findIndex(item => item.selected);
    if (currentStepIndex === -1) currentStepIndex = 0;

    function updateButtonState(stepIndex) {
        if (stepIndex === 0) {
            saveButton.textContent = "Next";
            saveButton.setAttribute("icon-start", "chevron-right");
        } else {
            saveButton.textContent = "Notify";
            saveButton.setAttribute("icon-start", "send");
        }
    }

    function goToNextStep() {
        if (currentStepIndex < stepperItems.length - 1) {
            stepperItems[currentStepIndex].selected = false;
            currentStepIndex++;
            stepperItems[currentStepIndex].selected = true;
            updateButtonState(currentStepIndex);
        }
    }

    stepper.addEventListener("calciteStepperItemSelect", function (event) {
        currentStepIndex = Array.from(stepperItems).findIndex(item => item.selected);
        updateButtonState(currentStepIndex);
    });

    updateButtonState(currentStepIndex);

    saveButton.addEventListener("click", function (event) {
        if (currentStepIndex === 0) {
            event.preventDefault();
            goToNextStep();
        } else {
            htmx.trigger(form, 'submit');
        }
    });
}

document.addEventListener("DOMContentLoaded", () => {
    init_DataTables();
    init_Charts();
    initD3();


    const logTableContainer = document.getElementById('log-table-container');
    // Check for a key element of the log filters AND the data script tag
    if (document.getElementById('log-form') && document.getElementById('log_initial_visible_cols_data')) {
        const logTableUrl = logTableContainer?.dataset.logTableUrl;
        if (logTableUrl) {
            init_LogFilters(logTableUrl);
        } else if (logTableContainer) { // Log table container exists, but URL missing
            console.warn("Log table URL not found on #log-table-container for init_LogFilters.");
        }
    }

    const content = document.getElementById("mainbodycontent");
    if (content) {
        content.hidden = false;
    } else {

    }
});

htmx.on('htmx:afterRequest', (e) => {
    if (e.detail.target.id === 'mainbodycontent') {
        init_DataTables();
        init_Charts();
        initD3();
        initPortalNotification();
        initPortalTools();
        content = document.getElementById('mainbodycontent');
        content.hidden = false;
        if (document.getElementById('log-form') && document.getElementById('log_initial_visible_cols_data')) {
            const logTableContainer = document.getElementById('log-table-container');
            const logTableUrl = logTableContainer?.dataset.logTableUrl;
            if (logTableUrl) {
                init_LogFilters(logTableUrl);
            } else if (logTableContainer) {
                console.warn("Log table URL not found for re-init after HTMX swap.");
            }
        }
    }
    if (e.detail.target.id === 'tool_settings_modal') {
        initPortalTools();
    }
    if (e.detail.target.id === 'webhook_settings_modal') {
        initWebhookSecretGenerator();
    }

    if (e.detail.target.id === 'service-table') {
        initializeSparklines();
    }

})

function externalTooltipHandler(context) {
    // Tooltip Element
    const {chart, tooltip} = context;
    const tooltipEl = getOrCreateTooltip(chart);

    // Hide if no tooltip
    if (tooltip.opacity === 0) {
        tooltipEl.classList.remove('visible');
        return;
    }

    // Set Text
    let innerHtml = '';
    if (tooltip.title && tooltip.title.length > 0) {
        innerHtml += `<div class="chartjs-tooltip-title">${tooltip.title[0]}</div>`;
    }
    if (tooltip.body) {
        const bodyLines = tooltip.body.map(b => b.lines);
        innerHtml += `<div class="chartjs-tooltip-body">${bodyLines.map(lines => lines.join('')).join('<br>')}</div>`;
    }

    tooltipEl.innerHTML = innerHtml;

    const {offsetLeft: positionX, offsetTop: positionY} = chart.canvas;

    // Position tooltip below the canvas
    const tooltipX = positionX + tooltip.caretX;
    const tooltipY = positionY + 48;

    tooltipEl.style.transform = `translate(${tooltipX}px, ${tooltipY}px) translateX(-50%)`;

    // Show tooltip
    tooltipEl.classList.add('visible');
}

function getOrCreateTooltip(chart) {
    let tooltipEl = chart.canvas.parentNode.querySelector('.chartjs-tooltip');

    if (!tooltipEl) {
        tooltipEl = document.createElement('div');
        tooltipEl.className = 'chartjs-tooltip';
        chart.canvas.parentNode.appendChild(tooltipEl);
    }

    return tooltipEl;
}

function initializeSparklines() {
    // Find all sparklines
    const sparklineElements = document.querySelectorAll('.sparkline-chart');

    sparklineElements.forEach(canvas => {
        const dataString = canvas.getAttribute('data-chart');
        const chartData = JSON.parse(dataString);

        // Extract values and labels
        const dataArray = chartData.values;
        const labels = chartData.dates || dataArray.map((_, index) => index);

        // Get Calcite colors
        const lineColor = getComputedStyle(document.body)
            .getPropertyValue('--calcite-color-text-1');
        const fillColor = getComputedStyle(document.body)
            .getPropertyValue('--calcite-color-text-3');

        const ctx = canvas.getContext('2d');

        new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    data: dataArray,
                    borderColor: lineColor || '#6b7280',
                    backgroundColor: fillColor + '20',
                    borderWidth: 0.5,
                    fill: true,
                    pointRadius: 0,
                    pointHoverRadius: 2,
                    tension: 0.1
                }]
            },
            options: {
                responsive: false,
                maintainAspectRatio: false,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        enabled: false,
                        external: externalTooltipHandler,
                        callbacks: {
                            title: function (context) {
                                return context[0].label;
                            },
                            label: function (context) {
                                return `Requests: ${context.parsed.y}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        display: false,
                        grid: {
                            display: false
                        }
                    },
                    y: {
                        display: false,
                        grid: {
                            display: false
                        }
                    }
                },
                elements: {
                    point: {
                        hoverBackgroundColor: lineColor || '#6b7280'
                    }
                }
            }
        });
    });
}

var activeWidget;
var shellPanel = document.getElementById("shell-panel-start");

var handleActionBarClick = ({target}) => {
    if (activeWidget) {
        document.querySelector(
            `[data-action-id=${activeWidget}]`
        ).active = false;
        document.querySelector(`[data-panel-id=${activeWidget}]`).hidden = true;
        shellPanel.collapsed = !shellPanel.collapsed;
    }

    var nextWidget = target.dataset.actionId;
    if (nextWidget !== activeWidget) {
        if (!target.getAttribute("data-action-id")) {
            activeWidget = null;
            return
        }
        nextWidgetElem = document.querySelector(`[data-panel-id=${nextWidget}]`);
        nextWidgetElem.active = true;
        nextWidgetElem.hidden = false;
        shellPanel.collapsed = false;
        activeWidget = nextWidget;
    } else {
        activeWidget = null;
    }
}

var actionBar = document.querySelector("calcite-action-bar");
var panel = document.querySelector("calcite-panel");
var actionBarExpanded = false;

if (actionBar) actionBar.addEventListener("click", handleActionBarClick);
if (panel) panel.addEventListener("click", handleActionBarClick);

document.addEventListener("calciteActionBarToggle", () => {
    actionBarExpanded = !actionBarExpanded;
});

htmx.on("closeModal", function (event) {
    (async () => {
        await customElements.whenDefined("calcite-dialog");
        let modal = document.querySelector("calcite-dialog[open=true], calcite-dialog[open=''], dialog[open]");
        if (modal?.componentOnReady) {
            await modal.componentOnReady();
        }
        if (modal) {
            modal.open = false;
        }
    })();

});

htmx.on("htmx:afterSettle", (e) => {
    (async () => {
        const modalMap = {
            "add_portal_modal": "add-modal",
            "update_portal_modal": "update-modal",
            "schedule_portal_modal": "schedule-modal",
            "progress-container": "credentials-modal"
        };
        const modalId = modalMap[e.detail.target.id];
        if (!modalId) return;

        const modal = document.getElementById(modalId);
        if (!modal) return;

        // Open the modal
        modal.open = true;

        // Wait for all Calcite components to be initialized
        await ensureCalciteComponentsInitialized(modal);


        // Attach event listeners scoped to this modal
        setupModalHTMXListeners(modal);
    })();

});

async function ensureCalciteComponentsInitialized(modal) {
    const calciteElements = modal.querySelectorAll("calcite-input, calcite-select, calcite-segmented-control");
    await Promise.all(Array.from(calciteElements).map(el => el.componentOnReady()));

    // Setup form interactions after elements are ready
    setupModalLogic(modal);
}

function setupModalLogic(modal) {
    const modalId = modal.id;

    if (["add-modal", "update-modal"].includes(modalId)) {
        setupPortalFormLogic(modal);
    } else if (modalId === "schedule-modal") {
        setupScheduleFormLogic(modal);
    } else if (modalId === "credentials-modal") {
        setupCredentialsFormLogic(modal);
    }
}

async function setupPortalFormLogic(modal) {
    const storePasswordControl = modal.querySelector("#store-password");
    if (!storePasswordControl) {
        return;
    }

    // Wait for the Calcite component to be ready
    await storePasswordControl.componentOnReady();

    const storePasswordSection = modal.querySelector("#store-password-container");
    if (!storePasswordSection) {
        return;
    }

    const usernameInput = modal.querySelector("calcite-input[name='username']");
    const passwordInput = modal.querySelector("calcite-input[name='password']");

    function toggleCredentials() {
        const storePasswordItem = storePasswordControl.querySelector("calcite-segmented-control-item[value='False']");
        if (!storePasswordItem) {
            return;
        }
        storePasswordSection.hidden = storePasswordItem.checked;
        usernameInput.disabled = storePasswordItem.checked;
        passwordInput.disabled = storePasswordItem.checked;
    }

    storePasswordControl.addEventListener("calciteSegmentedControlChange", toggleCredentials);
    toggleCredentials();
    const enableEmailControl = modal.querySelector("#enable-email");
    const adminEmailsContainer = modal.querySelector("#admin-emails-container");

    if (enableEmailControl && adminEmailsContainer) {
        enableEmailControl.addEventListener("calciteSegmentedControlChange", () => {
            const enabledItem = enableEmailControl.querySelector("calcite-segmented-control-item[value='False']");
            adminEmailsContainer.hidden = enabledItem.checked;
        });
    }

}

function setupScheduleFormLogic(modal) {
    const schedulePortalCancel = modal.querySelector("#schedule-portal-cancel");
    schedulePortalCancel.addEventListener("click", (e) => {
        e.preventDefault();
        modal.open = false;
    });

    const repeatType = modal.querySelector("#repeat-type");
    const endingOn = modal.querySelector("#ending-on");
    const allDayMinute = modal.querySelector("#all-day-minute");
    const startHourMinute = modal.querySelector("#between-hours-start-minute");
    const endHourMinute = modal.querySelector("#between-hours-end-minute");
    const allDayHour = modal.querySelector("#all-day-hour");
    const startHour = modal.querySelector("#between-hours-start-hour");
    const endHour = modal.querySelector("#between-hours-end-hour");
    const endDateContainer = modal.querySelector("#end-date-container");
    const endCountContainer = modal.querySelector("#end-count-container");

    function updateRepeatType() {
        const value = repeatType.value;
        modal.querySelector("#repeat-minute").hidden = value !== "minute";
        modal.querySelector("#repeat-hour").hidden = value !== "hour";
        modal.querySelector("#repeat-day").hidden = value !== "day";
        modal.querySelector("#repeat-week").hidden = value !== "week";
        modal.querySelector("#repeat-month").hidden = value !== "month";
    }

    function updateAllDay() {
        startHourMinute.disabled = allDayMinute.checked;
        endHourMinute.disabled = allDayMinute.checked;
        startHour.disabled = allDayHour.checked;
        endHour.disabled = allDayHour.checked;
    }

    function updateEndingOn() {
        endDateContainer.hidden = endingOn.value !== "date";
        // endCountContainer.hidden = endingOn.value !== "count";
    }

    repeatType.addEventListener("calciteSelectChange", updateRepeatType);
    endingOn.addEventListener("calciteSelectChange", updateEndingOn);
    allDayMinute.addEventListener("calciteSwitchChange", updateAllDay);
    allDayHour.addEventListener("calciteSwitchChange", updateAllDay);

    updateRepeatType();
    updateEndingOn();
}


function setupModalHTMXListeners(modal) {
    htmx.on(modal, "htmx:beforeRequest", (e) => {
        const clickedButton = e.detail.elt;
        clickedButton.loading = true;
    });

    htmx.on(modal, "htmx:afterRequest", (e) => {
        const clickedButton = e.detail.elt;
        clickedButton.loading = false;
    });

    // Ensure we clean up event listeners when modal closes
    modal.addEventListener("calciteDialogClose", () => {
        htmx.off(modal, "htmx:beforeRequest");
        htmx.off(modal, "htmx:afterRequest");
    });
}

function updateDarkMode() {
    document.body.classList.toggle("calcite-mode-dark");
}

const modeSwitch = document.querySelector("calcite-switch#mode-switch"); // Be more specific if multiple switches
if (modeSwitch) {
    modeSwitch.addEventListener("calciteSwitchChange", updateDarkMode);
}


async function showAlert(kind, label, message, autoClose = true) {
    const alert = document.createElement("calcite-alert");
    alert.setAttribute("kind", kind);
    alert.setAttribute("open", "");
    alert.setAttribute("auto-close", autoClose ? "true" : "false");
    alert.setAttribute("label", label);
    alert.setAttribute("icon", "");

    alert.innerHTML = `<div slot="message">${message}</div>`;

    await new Promise(resolve => setTimeout(resolve, 150));

    await customElements.whenDefined("calcite-dialog");

    let modal = document.querySelector("calcite-dialog[open], dialog[open]");
    if (modal?.componentOnReady) {
        await modal.componentOnReady();
    }
    if (modal) {
        alert.setAttribute("slot", "alerts");
        modal.appendChild(alert);
    }
    else {
        document.querySelector("#alert-container").appendChild(alert);
    }
}

htmx.on("showDangerAlert", (e) => {
    showAlert("danger", "Danger alert", e.detail.value, false).catch(console.error);
});

htmx.on("showSuccessAlert", (e) => {
    showAlert("success", "Success alert", e.detail.value, true).catch(console.error);
});

htmx.on("showInfoAlert", (e) => {
    showAlert("info", "Info alert", e.detail.value, true).catch(console.error);
});

htmx.on("showWarningAlert", (e) => {
    showAlert("warning", "Warning alert", e.detail.value, true).catch(console.error);
});

document.addEventListener('DOMContentLoaded', function () {
    refreshActivePortal();
});

document.addEventListener('click', function (event) {
    const closeButton = event.target.closest('[data-action="close-modal"]');
    if (closeButton) {
        event.preventDefault();
        const modal = closeButton.closest('calcite-dialog, dialog');
        if (modal) {
            if (typeof modal.setOpen === 'function') {
                modal.setOpen(false);
            } else {
                modal.open = false;
            }
        }
        if (closeButton.closest('calcite-alert')) {
            closeButton.closest('calcite-alert').remove();
        }
    }

    const openTrigger = event.target.closest('[data-modal-trigger]');
    if (openTrigger) {
        event.preventDefault();
        const modalId = openTrigger.dataset.modalTrigger;
        const modal = document.getElementById(modalId);
        if (modal) {
            if (typeof modal.setOpen === 'function') {
                modal.setOpen(true);
            } else {
                modal.open = true;
            }
        }
    }
});


function refreshActivePortal() {
    // Get the current URL
    var currentUrl = window.location.href;

    // Iterate over calcite-action elements
    document.querySelectorAll('[id^="portal-"]').forEach(function (element) {
        // Extract instance from the element ID
        var instance = element.id.split('-')[1];

        // Check if the instance matches the current URL
        if (currentUrl.includes('/enterpriseviz/portal/' + instance + '/')) {
            element.setAttribute('active', 'true');
        } else {
            element.removeAttribute('active', 'false')
        }
    });
}

function mergeTableRows(tableSelector, columnIndex) {
    let table = document.querySelector(tableSelector);
    if (!table) return;

    let rows = table.querySelectorAll("tbody tr");
    let prevCell = null;
    let rowspan = 1;

    rows.forEach((row, rowIndex) => {
        let cell = row.cells[columnIndex];
        if (!cell) return;

        if (prevCell && cell.innerText === prevCell.innerText) {
            rowspan++;
            prevCell.rowSpan = rowspan; // Increase rowspan of previous cell
            cell.classList.add("display-none"); // Hide duplicate cell
        } else {
            cell.classList.remove("display-none");
            prevCell = cell;
            rowspan = 1;
        }
    });
}

var blocks = document.querySelectorAll("calcite-block");

blocks?.forEach((el) => {
    el.addEventListener("calciteBlockToggle", function (event) {
        blocks?.forEach((block) => {
            block.open = block === event.target;
        });
    });
});

function initPortalTools() {
    const switches = document.querySelectorAll('calcite-switch');
    switches.forEach(async calciteSwitch => {
        if (calciteSwitch.componentOnReady) await calciteSwitch.componentOnReady(); // Wait for switch itself
        const hiddenInput = calciteSwitch.nextElementSibling;
        if (hiddenInput && hiddenInput.tagName === 'INPUT' && hiddenInput.type === 'hidden') {
            // Set initial value of hidden input based on switch state
            hiddenInput.value = calciteSwitch.checked ? 'True' : 'False';

            calciteSwitch.addEventListener('calciteSwitchChange', (event) => {
                hiddenInput.value = event.target.checked ? 'True' : 'False';
            });
        }
    });
}

function initWebhookSecretGenerator() {
    const generateButton = document.getElementById('generate-secret-action');
    const copyButton = document.getElementById('copy-secret-btn');

    if (generateButton) {
        generateButton.addEventListener('click', function() {
            // Generate a random 32-character secret
            const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
            let result = '';
            for (let i = 0; i < 32; i++) {
                result += chars.charAt(Math.floor(Math.random() * chars.length));
            }

            // Find the webhook secret input field
            const webhookSecretInput = document.querySelector('calcite-input[name="webhook_secret"]');
            if (webhookSecretInput) {
                webhookSecretInput.value = result;
            }
        });
    }

    if (copyButton) {
        copyButton.addEventListener('click', function() {
            const webhookSecretInput = document.querySelector('calcite-input[name="webhook_secret"]');
            if (webhookSecretInput && webhookSecretInput.value) {
                navigator.clipboard.writeText(webhookSecretInput.value).then(() => {
                    // Show success feedback
                    htmx.trigger(copyButton, 'showSuccessAlert', { value: 'Webhook secret copied to clipboard!' });
                }).catch(() => {
                    // Fallback for older browsers
                    webhookSecretInput.select();
                    document.execCommand('copy');
                    htmx.trigger(copyButton, 'showSuccessAlert', { value: 'Webhook secret copied to clipboard!' });

                });
            }
        });
    }
}

