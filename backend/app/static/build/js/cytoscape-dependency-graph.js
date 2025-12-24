/**
 * Initialize a Cytoscape dependency graph with Calcite styling
 * @param {string} containerId - ID of the container element (e.g., 'chartd3')
 * @param {Object} data - Graph data with nodes and links
 * @param {Array} data.nodes - Array of node objects with {id, name, type}
 * @param {Array} data.links - Array of link objects with {source, target}
 * @returns {Object} Cytoscape instance
 */
// TODO: Add filter by portal
// TODO: Add toggle to hide layers in service view
function initDependencyGraph(containerId, data) {
    // Ensure container exists
    const container = document.getElementById(containerId);
    if (!container) {
        console.warn(`initDependencyGraph: Container #${containerId} not found.`);
        return null;
    }

    // Auto-detect and parse graph data if not provided
    if (!data) {
        const dataEl = document.getElementById('graph-data');
        if (!dataEl) {
            console.warn('initDependencyGraph: #graph-data element not found.');
            return null;
        }

        try {
            data = JSON.parse(dataEl.textContent);
        } catch (err) {
            console.warn('initDependencyGraph: Failed to parse graph data JSON.', err);
            return null;
        }
    }

    // Validate data structure
    if (!data.nodes || !Array.isArray(data.nodes) || !Array.isArray(data.links)) {
        console.warn('initDependencyGraph: Invalid or missing graph data structure.');
        return null;
    }

    var iconPath = '/static/build/icons/'

    // Register the dagre layout with Cytoscape
    if (typeof cytoscapeDagre !== 'undefined') {
        cytoscape.use(cytoscapeDagre);
    }

    // Get Calcite CSS variables
    var computedStyle = getComputedStyle(document.body);

    // Helper to get Calcite color variable
    function getCalciteColor(varName) {
        return computedStyle.getPropertyValue(varName).trim();
    }

    // Calcite color palette
    let colors = {
        // UI colors
        borderColor: getCalciteColor('--calcite-color-border-1'),
        textPrimary: getCalciteColor('--calcite-color-text-1'),
        textSecondary: getCalciteColor('--calcite-color-text-3'),
        bgForeground: getCalciteColor('--calcite-color-background'),

        // Status colors
        linkNormal: getCalciteColor('--calcite-color-text-3'),
        linkDirect: getCalciteColor('--calcite-color-status-danger'),
        linkHighlight: getCalciteColor('--calcite-color-brand'),

        // Selection
        selectionBorder: getCalciteColor('--calcite-color-brand')
    };

    var iconSvgs = {
        layer: iconPath + 'layers-24.svg',
        service: iconPath + 'layer-service-24.svg',
        map: iconPath + 'map-24.svg',
        browser: iconPath + 'browser-24.svg'
    };

    // Helper function to get config for a type, defaulting to app/browser
    function getTypeConfig(type) {
        if (!type) return typeConfig.app;

        var normalizedType = type.toLowerCase();

        // Check if we have a specific config for this type
        if (typeConfig[normalizedType]) {
            return typeConfig[normalizedType];
        }

        // Default to app/browser for unknown types
        return {
            icon: 'browser',
            label: type
        };
    }


    // Type configurations for node styling
    var typeConfig = {
        layer: {
            icon: 'layer',
            label: 'Layer'
        },
        service: {
            icon: 'service',
            label: 'Service'
        },
        map: {
            icon: 'map',
            label: 'Map'
        },
        app: {
            icon: 'browser',
            label: 'App'
        }
    };

    // Build Cytoscape elements
    var elements = [];
    // Add nodes
    data.nodes.forEach(function (node) {
        var config = getTypeConfig(node.type);
        var name = node.name || '';
        var maxNameLen = 28;
        var shortName = name.length > maxNameLen ? name.substring(0, maxNameLen - 3) + '...' : name;
        var twoLineLabel = shortName + '\n' + config.label;
        elements.push({
            data: {
                id: node.id,
                label: twoLineLabel,
                name: shortName,
                type: node.type,
                typeLabel: config.label,
                icon: config.icon,
                iconUrl: iconSvgs[config.icon]
            }
        });
    });

    // Add edges and identify direct connections
    data.links.forEach(function (link) {
        var sourceNode = data.nodes.find(function (n) {
            return n.id === link.source;
        });
        var targetNode = data.nodes.find(function (n) {
            return n.id === link.target;
        });
        var isDirect = (
            sourceNode && targetNode &&
            sourceNode.type.toLowerCase() === 'service' &&
            targetNode.id.toLowerCase().includes('app-')
        );

        var edgeData = {
            id: link.source + '-' + link.target,
            source: link.source,
            target: link.target
        };

        if (isDirect) {
            edgeData.direct = true;
        }

        elements.push({data: edgeData});
    });

    // Initialize Cytoscape
    var cy = cytoscape({
        container: container,
        height: 400,

        elements: elements,

        style: [
            {
                selector: 'node',
                style: {
                    'width': 240,
                    'height': 44,
                    'shape': 'roundrectangle',
                    'corner-radius': '6px',
                    'background-color': colors.bgForeground,
                    'border-width': 1,
                    'border-color': colors.borderColor,

                    // Label
                    'label': 'data(label)',
                    'text-valign': 'center',
                    'text-halign': 'right',
                    'text-margin-x': -190,
                    'font-size': '12px',
                    'font-weight': '400',
                    'font-family': 'Avenir Next, Avenir, "Helvetica Neue", sans-serif',
                    'color': colors.textPrimary,
                    'text-wrap': 'wrap',
                    'text-max-width': 999,

                    'background-image': function(ele) { return ele.data('iconUrl'); },
                    'background-fit': 'contain',
                    'background-offset-x': '-90px',
                    'background-offset-y': '8px',
                    'background-opacity': 1,
                }
            },
            {
                selector: 'edge',
                style: {
                    'width': 1.5,
                    'line-color': colors.linkNormal,
                    'target-arrow-color': colors.linkNormal,
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'taxi',
                    'arrow-scale': 1,
                    'taxi-direction': 'horizontal',
                    'taxi-turn': '20px'
                }
            },
            {
                selector: 'edge[direct]',
                style: {
                    'line-color': colors.linkDirect,
                    'target-arrow-color': colors.linkDirect,
                    'line-style': 'dashed',
                    'line-dash-pattern': [6, 3]
                }
            },
            {
                selector: '.highlighted',
                style: {
                    'border-width': 4,
                    'border-color': colors.selectionBorder,
                    'z-index': 999
                }
            },
            {
                selector: '.dimmed',
                style: {
                    'opacity': 0.25
                }
            },
            {
                selector: '.edge-highlighted',
                style: {
                    'width': 2,
                    'line-color': colors.linkHighlight,
                    'target-arrow-color': colors.linkHighlight,
                    'z-index': 999,
                    'opacity': 1
                }
            }
        ],

        layout: {
            name: 'dagre',
            rankDir: 'LR',
            nodeSep: 10,
            rankSep: 100,
            fit: true
        },
        minZoom: 0.3,
        maxZoom: 3,
        wheelSensitivity: 0,
        userZoomingEnabled: true,
        userPanningEnabled: true,
        boxSelectionEnabled: false
    });

    // Selection handling
    var selectedNode = null;

    cy.on('tap', 'node', function (evt) {
        var node = evt.target;

        if (selectedNode === node) {
            clearSelection();
        } else {
            selectNode(node);
        }
    });

    cy.on('tap', function (evt) {
        if (evt.target === cy) {
            clearSelection();
        }
    });

    function selectNode(node) {
        selectedNode = node;

        var connectedNodes = new Set();
        var connectedEdges = new Set();

        connectedNodes.add(node);

        function traverseIncoming(n) {
            n.incomers('edge').forEach(function (edge) {
                connectedEdges.add(edge);
                var source = edge.source();
                if (!connectedNodes.has(source)) {
                    connectedNodes.add(source);
                    traverseIncoming(source);
                }
            });
        }

        function traverseOutgoing(n) {
            n.outgoers('edge').forEach(function (edge) {
                connectedEdges.add(edge);
                var target = edge.target();
                if (!connectedNodes.has(target)) {
                    connectedNodes.add(target);
                    traverseOutgoing(target);
                }
            });
        }

        traverseIncoming(node);
        traverseOutgoing(node);

        cy.elements().removeClass('highlighted dimmed edge-highlighted');

        cy.nodes().forEach(function (n) {
            if (connectedNodes.has(n)) {
                n.addClass('highlighted');
            } else {
                n.addClass('dimmed');
            }
        });

        cy.edges().forEach(function (e) {
            if (connectedEdges.has(e)) {
                e.addClass('edge-highlighted');
            } else {
                e.addClass('dimmed');
            }
        });
    }

    function clearSelection() {
        selectedNode = null;
        cy.elements().removeClass('highlighted dimmed edge-highlighted');
    }

    setTimeout(() => {
        cy.resize();
        cy.fit(null,10);
        cy.center();
    }, 100);

    // Handle Shadow DOM scroll issues with Calcite panels
    (async function () {
        try {
            var panel = document.getElementById('main-content-panel');
            if (!panel) {
                console.warn('Calcite panel #main-content-panel not found');
                return;
            }

            if (panel.componentOnReady) {
                await panel.componentOnReady();
            }

            var scrollable = null;
            if (panel.shadowRoot) {
                scrollable = panel.shadowRoot.querySelector('.content-wrapper');
            }

            if (!scrollable) {
                console.warn('Scrollable element .content-wrapper not found in panel shadow DOM');
                return;
            }

            scrollable.addEventListener('wheel', function (e) {
                var rect = container.getBoundingClientRect();
                var overCytoscape = e.clientX >= rect.left && e.clientX <= rect.right &&
                    e.clientY >= rect.top && e.clientY <= rect.bottom;

                if (overCytoscape) {
                    e.preventDefault();
                    e.stopPropagation();
                    return false;
                }
            }, {passive: false, capture: true});

            scrollable.addEventListener('scroll', function () {
                cy.resize();
            }, {passive: true});

            var resizeObserver = new ResizeObserver(function () {
                cy.resize();
            });
            resizeObserver.observe(container);

        } catch (err) {
            console.warn('Error setting up shadow DOM scroll handler:', err);
        }
    })();

    return {
        cy: cy,
        fit: function () {
            cy.fit(null,10);
        },
        reset: function () {
            cy.layout({name: 'dagre', rankDir: 'LR', nodeSep: 10, rankSep: 100, padding: 10}).run();
            cy.fit(null,10);
        },
        zoomIn: function () {
            cy.zoom(cy.zoom() * 1.2);
        },
        zoomOut: function () {
            cy.zoom(cy.zoom() * 0.8);
        },
        clearSelection: clearSelection,

        connectControls: function (buttonIds) {
            buttonIds = buttonIds || {
                reset: 'graph-reset-btn',
                center: 'graph-center-btn',
                zoomIn: 'graph-zoom-in-btn',
                zoomOut: 'graph-zoom-out-btn',
                fullscreen: 'graph-fullscreen-btn'
            };

            var self = this;
            var resetBtn = document.getElementById(buttonIds.reset);
            var centerBtn = document.getElementById(buttonIds.center);
            var zoomInBtn = document.getElementById(buttonIds.zoomIn);
            var zoomOutBtn = document.getElementById(buttonIds.zoomOut);
            var fullscreenBtn = document.getElementById(buttonIds.fullscreen);

            if (resetBtn) resetBtn.addEventListener('click', function () {
                self.reset();
            });
            if (centerBtn) centerBtn.addEventListener('click', function () {
                self.fit();
            });
            if (zoomInBtn) zoomInBtn.addEventListener('click', function () {
                self.zoomIn();
            });
            if (zoomOutBtn) zoomOutBtn.addEventListener('click', function () {
                self.zoomOut();
            });
            if (fullscreenBtn) fullscreenBtn.addEventListener('click', function () {
                self.toggleFullscreen();
            });
        },

        toggleFullscreen: function() {
            var panel = container.closest('.x_panel');
            if (!panel) {
                console.warn('Could not find .x_panel ancestor');
                return;
            }

            var fullscreenBtn = document.getElementById('graph-fullscreen-btn');

            if (!document.fullscreenElement) {
                // Enter fullscreen
                if (panel.requestFullscreen) {
                    panel.requestFullscreen();
                } else if (panel.webkitRequestFullscreen) {
                    panel.webkitRequestFullscreen();
                } else if (panel.msRequestFullscreen) {
                    panel.msRequestFullscreen();
                }

                // Add fullscreen styles
                panel.style.width = '100vw';
                panel.style.height = '100vh';
                panel.style.margin = '0';
                container.style.height = 'calc(100vh - 100px)';

                // Update icon
                if (fullscreenBtn) {
                    fullscreenBtn.setAttribute('icon', 'full-screen-exit');
                    fullscreenBtn.setAttribute('text', 'Exit Fullscreen');
                }

                // Resize graph to fit new container
                setTimeout(() => {
                    cy.resize();
                    cy.fit(null,10);
                }, 100);
            } else {
                // Exit fullscreen
                if (document.exitFullscreen) {
                    document.exitFullscreen();
                } else if (document.webkitExitFullscreen) {
                    document.webkitExitFullscreen();
                } else if (document.msExitFullscreen) {
                    document.msExitFullscreen();
                }

                // Reset styles
                panel.style.width = '';
                panel.style.height = '';
                panel.style.margin = '';
                container.style.height = '';

                // Update icon
                if (fullscreenBtn) {
                    fullscreenBtn.setAttribute('icon', 'extent');
                    fullscreenBtn.setAttribute('text', 'Fullscreen');
                }

                // Resize graph to fit original container
                setTimeout(() => {
                    cy.resize();
                    cy.fit(null, 10);
                }, 100);
            }
        }
    };
}
