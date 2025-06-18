/*
This file is part of Enterpriseviz.
Originally from  by Rob Schmuecker.
Modified by David C Jantz in 2025.
Licensed under GPLv3 (https://www.gnu.org/licenses/gpl-3.0.en.html).
*/

/*Copyright (c) 2013-2016, Rob Schmuecker
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* The name Rob Schmuecker may not be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL MICHAEL BOSTOCK BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.*/

function initD3() {
    if (document.getElementById('d3data')) {
        var data = JSON.parse(document.getElementById('d3data').textContent);
        var instances = [];
        var $select = $('<calcite-combobox placeholder="Instance" scale="s" selection-mode="single" placeholder-icon="filter"></calcite-combobox>');

        function extractInstance(node) {
            if (node.instance) {
                if (!instances.includes(node.instance)) {
                    instances.push(node.instance);
                }

            }
            if (node.children) {
                node.children.forEach(function (child) {
                    extractInstance(child);
                });
            }
        }

        extractInstance(data);

        for (const instance of instances) {
            if ($select.find('option[value=' + instance + ']').length == 0) {
                $select.append('<calcite-combobox-item value=' + instance + ' heading=' + instance + '></calcite-combobox-item>');
            }
        }
        $select.appendTo('#d3chart-filter');


        $select.on('calciteComboboxChange', function () {
            var selectedValue = $(this).val();
            var originalData = JSON.parse(document.getElementById('d3data').textContent);
            var filteredDatasets;

            if (selectedValue) {
                filteredDatasets = originalData.children.filter(function (dataset) {
                    return dataset.instance === selectedValue;
                });
            } else {
                filteredDatasets = originalData.children;
            }

            data.children = filteredDatasets;
            update(root);
            initialNode(root);
        });
// Calculate total nodes, max label length
        var totalNodes = 0;
        var maxLabelLength = 48;
// variables for drag/drop
        var selectedNode = null;
        var draggingNode = null;
// panning variables
        var panSpeed = 200;
        var panBoundary = 20; // Within 20px from edges will pan when dragging.
// Misc. variables
        var i = 0;
        var duration = 800;
        var root;

// size of the diagram
        var viewerWidth = 400;
        var viewerHeight = 370;

        var tree = d3.layout.tree();

// define a d3 diagonal projection for use by the node paths later on.
        var diagonal = d3.svg.diagonal()
            .projection(function (d) {
                return [d.y, d.x];
            });

// A recursive helper function for performing some setup by walking through all nodes

        function visit(parent, visitFn, childrenFn) {
            if (!parent) return;

            visitFn(parent);

            var children = childrenFn(parent);
            if (children) {
                var count = children.length;
                for (var i = 0; i < count; i++) {
                    visit(children[i], visitFn, childrenFn);
                }
            }
        }

// Call visit function to establish maxLabelLength
        visit(data, function (d) {
            totalNodes++;
            // maxLabelLength = Math.max(d.name.length + (d.instance == null ? 0 : d.instance.length), maxLabelLength); //d.instance == null ? d.name : d.instance + '/' + d.name

        }, function (d) {
            return d.children && d.children.length > 0 ? d.children : null;
        });


// sort the tree according to the node names

        function sortTree() {
            tree.sort(function (a, b) {
                return b.name.toLowerCase() < a.name.toLowerCase() ? 1 : -1;
            });
        }

// Sort the tree initially incase the JSON isn't in a sorted order.
        sortTree();

// TODO: Pan function, can be better implemented.

        function pan(domNode, direction) {
            var speed = panSpeed;
            if (panTimer) {
                clearTimeout(panTimer);
                translateCoords = d3.transform(svgGroup.attr("transform"));
                if (direction == 'left' || direction == 'right') {
                    translateX = direction == 'left' ? translateCoords.translate[0] + speed : translateCoords.translate[0] - speed;
                    translateY = translateCoords.translate[1];
                } else if (direction == 'up' || direction == 'down') {
                    translateX = translateCoords.translate[0];
                    translateY = direction == 'up' ? translateCoords.translate[1] + speed : translateCoords.translate[1] - speed;
                }
                scaleX = translateCoords.scale[0];
                scaleY = translateCoords.scale[1];
                scale = zoomListener.scale();
                svgGroup.transition().attr("transform", "translate(" + translateX + "," + translateY + ")scale(" + scale + ")");
                d3.select(domNode).select('g.node').attr("transform", "translate(" + translateX + "," + translateY + ")");
                zoomListener.scale(scale);
                zoomListener.translate([translateX, translateY]);
                panTimer = setTimeout(function () {
                    pan(domNode, speed, direction);
                }, 50);
            }
        }

// Define the zoom function for the zoomable tree

        function zoom() {
            svgGroup.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
        }


// define the zoomListener which calls the zoom function on the "zoom" event constrained within the scaleExtents
        var zoomListener = d3.behavior.zoom().scaleExtent([0.1, 3]).on("zoom", zoom);

        function initiateDrag(d, domNode) {
            draggingNode = d;
            d3.select(domNode).select('.ghostCircle').attr('pointer-events', 'none');
            d3.selectAll('.ghostCircle').attr('class', 'ghostCircle show');
            d3.select(domNode).attr('class', 'node activeDrag');

            svgGroup.selectAll("g.node").sort(function (a, b) {
                return a.id != draggingNode.id ? 1 : -1;
            });
            // if nodes has children, remove the links and nodes
            if (nodes.length > 1) {
                // remove link paths
                links = tree.links(nodes);
                nodePaths = svgzoomListener.scale(scale);
                zoomListener.translate([translateX, translateY]);
                Group.selectAll("path.link")
                    .data(links, function (d) {
                        return d.target.id;
                    }).remove();
                // remove child nodes
                nodesExit = svgGroup.selectAll("g.node")
                    .data(nodes, function (d) {
                        return d.id;
                    }).filter(function (d, i) {
                        if (d.id == draggingNode.id) {
                            return false;
                        }
                        return true;
                    }).remove();
            }

            // remove parent link
            parentLink = tree.links(tree.nodes(draggingNode.parent));
            svgGroup.selectAll('path.link').filter(function (d, i) {
                if (d.target.id == draggingNode.id) {
                    return true;
                }
                return false;
            }).remove();

            dragStarted = null;
        }

// define the baseSvg, attaching a class for styling and the zoomListener
        var baseSvg = d3.select("div#chartd3").append("svg")
            // .attr("width", viewerWidth)
            // .attr("height", viewerHeight)
            // .attr("viewBox", '0 0 400 400')
            .attr("height", '380')
            .attr('width', '100%')
            .attr("class", "overlay")
            .call(zoomListener);

// Helper functions for collapsing and expanding nodes.

        function collapse(d) {
            if (d.children) {
                d._children = d.children;
                d._children.forEach(collapse);
                d.children = null;
            }
        }

        function expand(d) {
            if (d._children) {
                d.children = d._children;
                d.children.forEach(expand);
                d._children = null;
            }
        }

// Function to center node when clicked/dropped so node doesn't get lost when collapsing/moving with large amount of children.

        function centerNode(source) {
            scale = zoomListener.scale();
            x = -source.y0;
            y = -source.x0;
            x = x * scale + viewerWidth / 2;
            y = y * scale + viewerHeight / 2;
            d3.select('g').transition()
                .duration(duration)
                .attr("transform", "translate(" + x + "," + y + ")scale(" + scale + ")");
            zoomListener.scale(scale);
            zoomListener.translate([x, y]);
        }

// Function to center node when clicked/dropped so node doesn't get lost when collapsing/moving with large amount of children.

        function initialNode(source) {
            scale = zoomListener.scale();
            x = -source.y0;
            y = -source.x0;
            x = x * scale + (root.name.length + (root.instance == null ? 0 : root.instance.length)) * 6 + 24;
            y = y * scale + viewerHeight / 2;
            d3.select('g').transition()
                .duration(duration)
                .attr("transform", "translate(" + x + "," + y + ")scale(" + scale + ")");
            zoomListener.scale(scale);
            zoomListener.translate([x, y]);
        }

// Toggle children function

        function toggleChildren(d) {
            if (d.children) {
                d._children = d.children;
                d.children = null;
            } else if (d._children) {
                d.children = d._children;
                d._children = null;
            }
            return d;
        }

// Toggle children on click.

        function click(d) {
            if (d3.event.defaultPrevented) return; // click suppressed
            d = toggleChildren(d);
            update(d);
            centerNode(d);
        }

        function update(source) {
            // Compute the new height, function counts total children of root node and sets tree height accordingly.
            // This prevents the layout looking squashed when new nodes are made visible or looking sparse when nodes are removed
            // This makes the layout more consistent.
            // Compute the new tree layout.
            var levelWidth = [1];
            var childCount = function (level, n) {

                if (n.children && n.children.length > 0) {
                    if (levelWidth.length <= level + 1) levelWidth.push(0);

                    levelWidth[level + 1] += n.children.length;
                    n.children.forEach(function (d) {
                        childCount(level + 1, d);
                    });
                }
            };
            childCount(0, root);
            var newHeight = d3.max(levelWidth) * 18; // 25 pixels per line
            tree = tree.size([newHeight, viewerWidth]);
            var nodes = tree.nodes(root).reverse(),
                links = tree.links(nodes);

            // Normalize for fixed-depth.
            nodes.forEach(function (d) {
                d.y = (d.depth * (maxLabelLength * 6 + 20));
            });

            // Update the nodes…
            var node = svgGroup.selectAll("g.node")
                .data(nodes, function (d) {
                    return d.id || (d.id = ++i);
                });

            // Enter any new nodes at the parent's previous position.
            var nodeEnter = node.enter().append("g")
                .attr("class", "node")
                .attr("transform", function (d) {
                    return "translate(" + source.y0 + "," + source.x0 + ")";
                })
                .on("click", click)
            nodeEnter.append("circle")
                .append("circle")
                .attr("r", 2.5)
                .attr("fill", d => (d._children ? "var(--calcite-ui-brand)" : "var(--calcite-ui-foreground-1)"))
                .attr("stroke-width", 10);
            nodeEnter.append("text")
                .attr("x", -10)
                .attr("dy", ".35em")
                .attr("text-anchor", "end")
                .text(function (d) {
                    return d.instance == null ? d.name : d.instance + '/' + d.name;
                })
                .style("fill-opacity", 1e-6);
            // Transition nodes to their new position.
            var nodeUpdate = node.transition()
                .duration(duration)
                .attr("transform", function (d) {
                    return "translate(" + d.y + "," + d.x + ")";
                });
            nodeUpdate.select("circle")
                .attr("r", 4.5)
                .style("fill", function (d) {
                    return d._children ? "var(--calcite-ui-brand)" : "var(--calcite-ui-foreground-1)";
                });

            nodeUpdate.select("text")
                .style("fill-opacity", 1);

            // Transition exiting nodes to the parent's new position.
            var nodeExit = node.exit().transition()
                .duration(duration)
                .attr("transform", function (d) {
                    return "translate(" + source.y + "," + source.x + ")";
                })
                .remove();

            nodeExit.select("circle")
                .attr("r", 1e-6);

            nodeExit.select("text")
                .style("fill-opacity", 1e-6);

            // Update the links…
            var link = svgGroup.selectAll("path.link")
                .data(links, function (d) {
                    return d.target.id;
                });

            // Enter any new links at the parent's previous position.
            link.enter().insert("path", "g")
                .attr("class", "link")
                .attr("d", function (d) {
                    var o = {x: source.x0, y: source.y0};
                    return diagonal({source: o, target: o});
                });

            // Transition links to their new position.
            link.transition()
                .duration(duration)
                .attr("d", diagonal);

            // Transition exiting nodes to the parent's new position.
            link.exit().transition()
                .duration(duration)
                .attr("d", function (d) {
                    var o = {x: source.x, y: source.y};
                    return diagonal({source: o, target: o});
                })
                .remove();

            // Stash the old positions for transition.
            nodes.forEach(function (d) {
                d.x0 = d.x;
                d.y0 = d.y;
            });
        }

// Append a group which holds all nodes and which the zoom Listener can act upon.
        var svgGroup = baseSvg.append("g").attr("transform", "translate(" + viewerWidth / 2 + "," + viewerHeight / 2 + ")");


// Define the root
        root = data;
        root.x0 = viewerHeight / 2;
        root.y0 = 0;

// Layout the tree initially and center on the root node.
        update(root);
        initialNode(root);
    }

}

// window.onload = function () {
//     initD3();
// }
// if (document.getElementById('d3data')) {
//     initD3();
// }
