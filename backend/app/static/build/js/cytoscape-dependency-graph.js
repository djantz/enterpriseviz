/**
 * Initialize a Cytoscape dependency graph with Calcite styling
 * @param {string} containerId - ID of the container element (e.g., 'chartd3')
 * @param {Object} data - Graph data with nodes and links
 * @param {Array} data.nodes - Array of node objects with {id, name, type}
 * @param {Array} data.links - Array of link objects with {source, target}
 * @returns {Object} Cytoscape instance
 */
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

    var svgHeader = '<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE svg>';

    // Inline SVG icons as data URIs
    var iconSvgs = {
        layer: 'data:image/svg+xml;utf8,' + encodeURIComponent(
            svgHeader + `<svg width="24" height="24" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path fill="${colors.textPrimary}" d="M12.207 13.2A6.624 6.624 0 0 1 16 14.29L13.867 1.687a8.205 8.205 0 0 0-3.161-.674c-2.69 0-2.724.986-5.412.986a10.39 10.39 0 0 1-3.161-.512L0 14.09a9.158 9.158 0 0 0 3.793.711c3.665 0 4.749-1.6 8.414-1.6zM2.935 2.74A11.376 11.376 0 0 0 5.294 3a7.833 7.833 0 0 0 3.045-.552 5.895 5.895 0 0 1 2.367-.434 7.213 7.213 0 0 1 2.27.399l1.72 10.163a8.317 8.317 0 0 0-2.489-.376 12.79 12.79 0 0 0-4.526.852 10.962 10.962 0 0 1-3.888.748 9.54 9.54 0 0 1-2.668-.363z"/><path fill="none" d="M0 0h16v16H0z"/></svg>`),
        service: 'data:image/svg+xml;utf8,' + encodeURIComponent(
            svgHeader + `<svg width="24" height="24" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path fill="${colors.textPrimary}" d="M10.706 1.014a7.215 7.215 0 0 1 2.27.399l1.72 10.163a8.317 8.317 0 0 0-2.489-.376 12.79 12.79 0 0 0-4.526.852l-.604.204-.363.108a9.678 9.678 0 0 1-2.921.436 9.54 9.54 0 0 1-2.668-.363L2.935 1.74A11.372 11.372 0 0 0 5.294 2a7.833 7.833 0 0 0 3.045-.552 5.894 5.894 0 0 1 2.367-.434zm0-1C8.016.014 7.982 1 5.294 1A10.39 10.39 0 0 1 2.133.488L0 13.09a9.158 9.158 0 0 0 3.793.711A10.667 10.667 0 0 0 7 13.322V15H2v1h11v-1H8v-2a11.89 11.89 0 0 1 4.207-.8A6.624 6.624 0 0 1 16 13.29L13.867.687a8.205 8.205 0 0 0-3.161-.674z"/><path fill="none" d="M0 0h16v16H0z"/></svg>`),
        map: 'data:image/svg+xml;utf8,' + encodeURIComponent(
            svgHeader + `<svg width="24" height="24" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path fill="${colors.textPrimary}" d="M1 1v14h14V1zm9.082 13c-.005-.037-.113-.274-.122-.309a1.498 1.498 0 0 0 .718-1.267 1.24 1.24 0 0 0-.053-.358 1.594 1.594 0 0 0 .326-.607c.03.002.062.003.093.003a1.415 1.415 0 0 0 .836-.266 1.517 1.517 0 0 0 .126.124 2.385 2.385 0 0 0 1.564.68H14v2zM14 11h-.43a1.464 1.464 0 0 1-.906-.433c-.264-.23-.258-.782-.617-.782-.482 0-.52.677-1.003.677-.219 0-.38-.177-.599-.177-.193 0-.445.102-.445.293v.502c0 .424-.506.508-.506.934 0 .171.184.236.184.41 0 .58-.893.502-.893 1.08 0 .191.215.305.215.486V14H6.375a1.545 1.545 0 0 0 .09-.502c0-.547-1.043-.393-1.207-.72-.407-.813.693-1.022.693-1.673 0-.16-.082-.488-.334-.513-.351-.035-.443.154-.797.154a.406.406 0 0 1-.437-.36c0-.386.308-.566.308-.952 0-.25-.102-.393-.102-.643a.619.619 0 0 1 .59-.643c.323 0 .464.264.618.54a.642.642 0 0 0 .617.308c.49 0 .798-.61.798-.977a.471.471 0 0 1 .437-.488c.347 0 .476.36.824.36.57 0 .55-.756 1.053-1.03.618-.332.438-1.052.36-1.44-.032-.169.29-.5.464-.487.72.05.412-.54.412-.823a.434.434 0 0 1 .022-.142c.111-.332.595-.438.595-.836 0-.281-.233-.41-.233-.693a.653.653 0 0 1 .22-.44H14zM2 14V2h8.278c-.013.077-.132.356-.132.44a1.496 1.496 0 0 0 .112.567 1.6 1.6 0 0 0-.422.643 1.428 1.428 0 0 0-.074.442 1.676 1.676 0 0 0-.536.43 1.317 1.317 0 0 0-.32 1.091 3.213 3.213 0 0 1 .066.414 1.987 1.987 0 0 0-.649.67 1.462 1.462 0 0 0-.674-.166 1.447 1.447 0 0 0-1.383 1.086 1.443 1.443 0 0 0-1.086-.469 1.62 1.62 0 0 0-1.591 1.643c0 .254.113.293.084.574s-.29.535-.29 1.022a1.371 1.371 0 0 0 .984 1.29 1.583 1.583 0 0 0-.003 1.549c.143.286.636.774.534.774z"/><path fill="none" d="M0 0h16v16H0z"/></svg>`),
        browser: 'data:image/svg+xml;utf8,' + encodeURIComponent(
            svgHeader + `<svg width="24" height="24" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path fill="${colors.textPrimary}" d="M0 1v14h16V1zm1 13V5h4v9zm14 0H6V5h9zm0-10h-1V3h-1v1H1V2h14z"/><path fill="none" d="M0 0h16v16H0z"/></svg>`)
    };

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
        var config = typeConfig[node.type] || typeConfig.layer;
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
                icon: config.icon,
                iconUrl: iconSvgs[config.icon]
            }
        });
    });
    console.log(colors.textPrimary);
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
            targetNode.type.toLowerCase() === 'app'
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
                    'taxi-turn': '25%'
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

            console.log('Shadow DOM scroll handler attached to .content-wrapper successfully');
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
                var fullscreenBtn = document.getElementById('graph-fullscreen-btn');
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
                var fullscreenBtn = document.getElementById('graph-fullscreen-btn');
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
