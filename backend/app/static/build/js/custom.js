$('.collapse-link').on('click', function () {
    var $BOX_PANEL = $(this).closest('.x_panel'),
        $ICON = $(this).find('calcite-icon'),
        $BOX_CONTENT = $BOX_PANEL.find('.x_content');

    // fix for some div with hardcoded fix class
    if ($BOX_PANEL.attr('style')) {
        $BOX_CONTENT.slideToggle(200, function () {
            $BOX_PANEL.removeAttr('style');
        });
    } else {
        $BOX_CONTENT.slideToggle(200);
        $BOX_PANEL.css('height', 'auto');
    }
    var currentIcon = $ICON.attr('icon');
    var newIcon = (currentIcon === 'chevron-up') ? 'chevron-down' : 'chevron-up';

    $ICON.attr('icon', newIcon);
});


$('.close-link').click(function () {
    var $BOX_PANEL = $(this).closest('.x_panel');

    $BOX_PANEL.remove();
});

window.addEventListener('unload', function () {
    document.documentElement.innerHTML = '';
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
                            extend: "copy",
                            tag: 'calcite-button',
                            attr: {scale: "s", kind: "inverse", "icon-start": "copy-to-clipboard"}
                        },
                        {
                            extend: "csv",
                            tag: 'calcite-button',
                            attr: {scale: "s", kind: "inverse", "icon-start": "file-csv"}
                        },
                        {
                            extend: "excel",
                            tag: 'calcite-button',
                            attr: {scale: "s", kind: "inverse", "icon-start": "file-excel"}
                        },
                        {
                            extend: "print",
                            tag: 'calcite-button',
                            attr: {scale: "s", kind: "inverse", "icon-start": "print"}
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
                }
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
                        $select.append(`<calcite-combobox-item value="${val}" text-label="${val}"></calcite-combobox-item>`);
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
        const borderColor = getComputedStyle(document.body).getPropertyValue('--calcite-ui-border-3');
        const fontColor = getComputedStyle(document.body).getPropertyValue('--calcite-ui-text-2');
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
            $select.append(`<calcite-combobox-item value="${instance}" text-label="${instance}"></calcite-combobox-item>`);
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
function customColors() {
    // Define an array of custom colors
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

document.addEventListener("DOMContentLoaded", () => {
    init_DataTables();
    init_Charts();
    initD3();
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
        content = document.getElementById('mainbodycontent');
        content.hidden = false;
    }
    $('.sparkline').sparkline('html', {
        type: 'line',
        width: '110',
        height: '20',
        lineColor: getComputedStyle(document.body)
            .getPropertyValue('--calcite-ui-text-3'),
        fillColor: getComputedStyle(document.body)
            .getPropertyValue('--calcite-ui-text-3'),
        lineWidth: 0.5,
        spotColor: null,
        minSpotColor: null,
        maxSpotColor: null,
        highlightSpotColor: null,
        highlightLineColor: null,
        spotRadius: 2,
        normalRangeColor: '#ffffff',
        drawNormalOnTop: false,
        tooltipContainer: "#mainbodycontent",
        chartRangeMinX: 0
    })
})

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

        // Now safely access the components
        let modal = await document.querySelector("calcite-dialog[open], dialog[open]")?.componentOnReady();
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

    const cancelButton = modal.querySelector("#add-portal-cancel, #update-portal-cancel");
    if (cancelButton) {
        cancelButton.addEventListener("click", (e) => {
            e.preventDefault();
            modal.open = false;
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

function setupCredentialsFormLogic(modal) {
    const cancelButton = modal.querySelector("#credentials-portal-cancel");
    cancelButton.addEventListener("click", (e) => {
        e.preventDefault();
        modal.open = false;
    });
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
    var modeswitch = document.querySelector("calcite-switch");
    document.body.classList.toggle("calcite-mode-dark");
}

modeSwitch = document.querySelector("calcite-switch")
if (modeSwitch) {
    modeSwitch.addEventListener("calciteSwitchChange", updateDarkMode)
}


htmx.on("showDangerAlert", (e) => {
    const alert = document.createElement("calcite-alert");
    alert.setAttribute("kind", "danger");
    alert.setAttribute("open", "");
    alert.setAttribute("autoClose", "false");
    alert.setAttribute("label", "Danger alert");
    alert.setAttribute("icon", "");

    // Correctly interpolate e.detail.value
    alert.innerHTML = `<div slot="message">${e.detail.value}</div>`;
    (async () => {
        await customElements.whenDefined("calcite-dialog");

        // Now safely access the components
        let modal = await document.querySelector("calcite-dialog[open], dialog[open]")?.componentOnReady();
        if (modal) {
            alert.setAttribute("slot", "alerts");
            modal.appendChild(alert);
        }
    })();

    document.querySelector("#alert-container").appendChild(alert);
});
htmx.on("showSuccessAlert", (e) => {
    const alert = document.createElement("calcite-alert");
    alert.setAttribute("kind", "success");
    alert.setAttribute("open", "");
    alert.setAttribute("autoClose", "true");
    alert.setAttribute("label", "Success alert");
    alert.setAttribute("icon", "");


    // Correctly interpolate e.detail.value
    alert.innerHTML = `<div slot="message">${e.detail.value}</div>`;

    document.querySelector("#alert-container").appendChild(alert);
});
htmx.on("showInfoAlert", (e) => {
    const alert = document.createElement("calcite-alert");
    alert.setAttribute("kind", "brand");
    alert.setAttribute("open", "");
    alert.setAttribute("autoClose", "true");
    alert.setAttribute("label", "Info alert");
    alert.setAttribute("icon", "");


    // Correctly interpolate e.detail.value
    alert.innerHTML = `<div slot="message">${e.detail.value}</div>`;

    document.querySelector("#alert-container").appendChild(alert);
});

document.addEventListener('DOMContentLoaded', function () {
    refreshActivePortal();
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
    if (!table) return; // Exit if table not found

    let rows = table.querySelectorAll("tbody tr");
    let prevCell = null;
    let rowspan = 1;

    rows.forEach((row, rowIndex) => {
        let cell = row.cells[columnIndex];

        if (prevCell && cell.innerText === prevCell.innerText) {
            rowspan++;
            prevCell.rowSpan = rowspan; // Increase rowspan of previous cell
            cell.style.display = "none"; // Hide duplicate cell
        } else {
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

// document.addEventListener("htmx:confirm", function (e) {
//     // The event is triggered on every trigger for a request, so we need to check if the element
//     // that triggered the request has a hx-confirm attribute, if not we can return early and let
//     // the default behavior happen
//     if (!e.detail.target.hasAttribute('hx-confirm')) return
//     // This will prevent the request from being issued to later manually issue it
//     e.preventDefault()
//
//     const confirm = document.createElement("calcite-dialog");
//     confirm.setAttribute("modal", "true");
//     confirm.setAttribute("open", "");
//     confirm.setAttribute("width", "s");
//     confirm.setAttribute("heading", "Confirm Deletion");
//     confirm.setAttribute("icon", "");
//
//
//     // Correctly interpolate e.detail.value
//     confirm.innerHTML = `<div>Are you sure you want to delete ${e.detail.question}? This action cannot be undone</div>
//                         <calcite-button slot="footer-start" appearance="outline" kind="neutral">Cancel</calcite-button>
//                         <calcite-button slot="footer-end" appearance="solid" kind="danger" onClick="e.detail.issueRequest(true)">Delete</calcite-button>`;
//
//
//     document.querySelector("#alert-container").appendChild(confirm);
//     // Swal.fire({
//     //   title: "Proceed?",
//     //   text: `I ask you... ${e.detail.question}`
//     // }).then(function(result) {
//     //   if (result.isConfirmed) {
//     //     // If the user confirms, we manually issue the request
//     //     e.detail.issueRequest(true); // true to skip the built-in window.confirm()
//     //   }
//     // })
// })
