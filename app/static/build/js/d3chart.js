const data = JSON.parse(document.getElementById('d3data').textContent);

root = data;
var containerWidth = d3.select('div#chartd3').style('width').slice(0, -2)

var containerHeight = d3.select('div#chartd3').style('height').slice(0, -2)
console.log(containerHeight)
console.log(containerWidth)
var textLength = root.name.length;

var margin = {top: 10, right: 0, bottom: 10, left: textLength * 9}
width = containerWidth - margin.right - margin.left,
    height = containerHeight - 20;

var nodeWidth = Math.min((width / 3) - 10, 360)


var i = 0,
    duration = 300,
    root;

var tree = d3.layout.tree()
    .size([height, width]);

var diagonal = d3.svg.diagonal()
    .projection(function (d) {
        return [d.y, d.x];
    });

var svg = d3.select("div#chartd3")

    .append("svg")
    // Responsive SVG needs these 2 attributes and no width and height attr.
    .attr("width", '100%')
    .attr("height", '100%')
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

root.x0 = height / 2;
root.y0 = 0;

function collapse(d) {
    if (d.children) {
        d._children = d.children;
        d._children.forEach(collapse);
        d.children = null;
    }
}

root.children.forEach(collapse);
update(root);
//});

d3.select(self.frameElement);

// .style("height", "800px");

function update(source) {

    // Compute the new tree layout.
    var nodes = tree.nodes(root).reverse(),
        links = tree.links(nodes);

    // Normalize for fixed-depth.
    nodes.forEach(function (d) {
        d.y = d.depth * nodeWidth;
    });

    // Update the nodes…
    var node = svg.selectAll("g.node")
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
    // .on("mouseover", function (d) {
    //     d3.select(this) // The node
    //     // The class is used to remove the additional text later
    //         .append('text')
    //         .style("pointer-events", "none")
    //         .classed('info-owner', true)
    //         .attr('x', 20)
    //         .attr('y', 10)
    //         .text(function (d) {
    //             return d.type == null
    //                 ? "Type: " + d.type
    //                 : null;
    //         });
    //     d3.select(this)
    //         .append('text')
    //         .style("pointer-events", "none")
    //         .classed('info-created', true)
    //         .attr('x', 20)
    //         .attr('y', 20)
    //         .text(function (d) {
    //             return d.created == null
    //                 ? "URL: " + d.url
    //                 : null;
    //
    //
    //         });
    //     d3.select(this)
    //         .append('text')
    //         .style("pointer-events", "none")
    //         .classed('info-modified', true)
    //         .attr('x', 20)
    //         .attr('y', 30)
    //         .text(function (d) {
    //             return d.modified == null
    //                 ? null
    //                 : null;
    //         });
    // })
    // .on("mouseout", function () {
    //     // Remove the info text on mouse out.
    //     d3.select(this)
    //         .select('text.info-owner')
    //         .remove();
    //     d3.select(this)
    //         .select('text.info-created')
    //         .remove();
    //     d3.select(this)
    //         .select('text.info-modified')
    //         .remove();
    // });

    nodeEnter.append("circle")
        .append("circle")
        .attr("r", 2.5)
        .attr("fill", d => (d._children ? "#555" : "#999"))
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
            return d._children ? "#343a40" : "#fff";
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
    var link = svg.selectAll("path.link")
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

// Toggle children on click.
function click(d) {
    if (d.children) {
        d._children = d.children;
        d.children = null;
    } else {
        d.children = d._children;
        d._children = null;
    }
    /*
    root.children.forEach(function(node) {
        console.log(node, d, node === d);
        //collapse(node);
    });*/
    update(d);
}

