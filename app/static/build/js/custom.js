/**
 * Resize function without multiple trigger
 *
 * Usage:
 * $(window).smartresize(function(){
 *     // code here
 * });
 */
(function ($, sr) {
    // debouncing function from John Hann
    // http://unscriptable.com/index.php/2009/03/20/debouncing-javascript-methods/
    var debounce = function (func, threshold, execAsap) {
        var timeout;

        return function debounced() {
            var obj = this,
                args = arguments;

            function delayed() {
                if (!execAsap)
                    func.apply(obj, args);
                timeout = null;
            }

            if (timeout)
                clearTimeout(timeout);
            else if (execAsap)
                func.apply(obj, args);

            timeout = setTimeout(delayed, threshold || 100);
        };
    };

    // smartresize
    jQuery.fn[sr] = function (fn) {
        return fn ? this.bind('resize', debounce(fn)) : this.trigger(sr);
    };
})(jQuery, 'smartresize');

/**
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

var CURRENT_URL = window.location.href.split('#')[0].split('?')[0],
    $BODY = $('body'),
    $MENU_TOGGLE = $('#menu_toggle'),
    $SIDEBAR_MENU = $('#sidebar-menu'),
    $SIDEBAR_FOOTER = $('.sidebar-footer.hidden-md'),
    $LEFT_COL = $('.left_col'),
    $RIGHT_COL = $('.right_col'),
    $NAV_MENU = $('.nav_menu'),
    $FOOTER = $('footer');


// Sidebar
function init_sidebar() {
    // TODO: This is some kind of easy fix, maybe we can improve this
    var setContentHeight = function () {
        // reset height
        $RIGHT_COL.css('min-height', $(window).height());

        var bodyHeight = $BODY.outerHeight(),
            footerHeight = $BODY.hasClass('footer_fixed') ? -10 : $FOOTER.height(),
            leftColHeight = $LEFT_COL.eq(1).height() + $SIDEBAR_FOOTER.height(),
            contentHeight = bodyHeight < leftColHeight ? leftColHeight : bodyHeight;

        // normalize content
        contentHeight -= $NAV_MENU.height() + footerHeight;

        $RIGHT_COL.css('min-height', contentHeight);
    };

    $SIDEBAR_MENU.find('a').on('click', function (ev) {
        var $li = $(this).parent();

        if ($li.is('.active')) {
            $li.removeClass('active active-sm');
            $('ul:first', $li).slideUp(function () {
                setContentHeight();
            });
        } else {
            // prevent closing menu if we are on child menu
            if (!$li.parent().is('.child_menu')) {
                $SIDEBAR_MENU.find('li').removeClass('active active-sm');
                $SIDEBAR_MENU.find('li ul').slideUp();
            } else {
                if ($BODY.is(".nav-sm")) {
                    $SIDEBAR_MENU.find("li").removeClass("active active-sm");
                    $SIDEBAR_MENU.find("li ul").slideUp();
                }
            }
            $li.addClass('active');

            $('ul:first', $li).slideDown(function () {
                setContentHeight();
            });
        }
    });
    $SIDEBAR_FOOTER.find('a').on('click', function (ev) {
        var $li = $(this).parent();

        if ($li.is('.active')) {
            $li.removeClass('active active-sm');
            $('ul:first', $li).slideUp(function () {
                setContentHeight();
            });
        } else {
            // prevent closing menu if we are on child menu
            if (!$li.parent().is('.child_menu')) {
                $SIDEBAR_FOOTER.find('li').removeClass('active active-sm');
                $SIDEBAR_FOOTER.find('li ul').slideUp();
            } else {
                if ($BODY.is(".nav-sm")) {
                    $SIDEBAR_FOOTER.find("li").removeClass("active active-sm");
                    $SIDEBAR_FOOTER.find("li ul").slideUp();
                }
            }
            $li.addClass('active');

            $('ul:first', $li).slideDown(function () {
                setContentHeight();
            });
        }
    });


    // toggle small or large menu
    $MENU_TOGGLE.on('click', function () {
        if ($BODY.hasClass('nav-md')) {
            $SIDEBAR_MENU.find('li.active ul').hide();
            $SIDEBAR_MENU.find('li.active').addClass('active-sm').removeClass('active');
        } else {
            $SIDEBAR_MENU.find('li.active-sm ul').show();
            $SIDEBAR_MENU.find('li.active-sm').addClass('active').removeClass('active-sm');
        }

        $BODY.toggleClass('nav-md nav-sm');

        setContentHeight();
    });

    // check active menu
    $SIDEBAR_MENU.find('a[href="' + CURRENT_URL + '"]').parent('li').addClass('current-page');

    if ($BODY.hasClass('nav-md')) {
        $SIDEBAR_MENU.find('a').filter(function () {
            return this.href == CURRENT_URL;
        }).parent('li').addClass('current-page').parents('ul').slideDown(function () {
            setContentHeight();
        }).parent().addClass('active')
    }

    // recompute content when resizing
    $(window).smartresize(function () {
        setContentHeight();
    });

    setContentHeight();

    // fixed sidebar
    if ($.fn.mCustomScrollbar) {
        $('.menu_fixed').mCustomScrollbar({
            autoHideScrollbar: true,
            theme: 'minimal',
            mouseWheel: {preventDefault: true}
        });
    }
};
// /Sidebar

var randNum = function () {
    return (Math.floor(Math.random() * (1 + 40 - 20))) + 20;
};

// Panel toolbox
$(document).ready(function () {
    $('.collapse-link').on('click', function () {
        var $BOX_PANEL = $(this).closest('.x_panel'),
            $ICON = $(this).find('i'),
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

        $ICON.toggleClass('fa-chevron-up fa-chevron-down');
    });

    $('.close-link').click(function () {
        var $BOX_PANEL = $(this).closest('.x_panel');

        $BOX_PANEL.remove();
    });
});

/* PNotify */

function init_PNotify() {

    if (typeof (PNotify) === 'undefined') {
        return;
    }
    console.log('init_PNotify');

    new PNotify({
        title: "PNotify",
        type: "info",
        text: "Welcome. Try hovering over me. You can click things behind me, because I'm non-blocking.",
        nonblock: {
            nonblock: true
        },
        addclass: 'dark',
        styling: 'bootstrap3',
        hide: false,
        before_close: function (PNotify) {
            PNotify.update({
                title: PNotify.options.title + " - Enjoy your Stay",
                before_close: null
            });

            PNotify.queueRemove();

            return false;
        }
    });

};


/* CUSTOM NOTIFICATION */

function init_CustomNotification() {

    console.log('run_customtabs');

    if (typeof (CustomTabs) === 'undefined') {
        return;
    }
    console.log('init_CustomTabs');

    var cnt = 10;

    TabbedNotification = function (options) {
        var message = "<div id='ntf" + cnt + "' class='text alert-" + options.type + "' style='display:none'><h2><i class='fa fa-bell'></i> " + options.title +
            "</h2><div class='close'><a href='javascript:;' class='notification_close'><i class='fa fa-close'></i></a></div><p>" + options.text + "</p></div>";

        if (!document.getElementById('custom_notifications')) {
            alert('doesnt exists');
        } else {
            $('#custom_notifications ul.notifications').append("<li><a id='ntlink" + cnt + "' class='alert-" + options.type + "' href='#ntf" + cnt + "'><i class='fa fa-bell animated shake'></i></a></li>");
            $('#custom_notifications #notif-group').append(message);
            cnt++;
            CustomTabs(options);
        }
    };

    CustomTabs = function (options) {
        $('.tabbed_notifications > div').hide();
        $('.tabbed_notifications > div:first-of-type').show();
        $('#custom_notifications').removeClass('dsp_none');
        $('.notifications a').click(function (e) {
            e.preventDefault();
            var $this = $(this),
                tabbed_notifications = '#' + $this.parents('.notifications').data('tabbed_notifications'),
                others = $this.closest('li').siblings().children('a'),
                target = $this.attr('href');
            others.removeClass('active');
            $this.addClass('active');
            $(tabbed_notifications).children('div').hide();
            $(target).show();
        });
    };

    CustomTabs();

    var tabid = idname = '';

    $(document).on('click', '.notification_close', function (e) {
        idname = $(this).parent().parent().attr("id");
        tabid = idname.substr(-2);
        $('#ntf' + tabid).remove();
        $('#ntlink' + tabid).parent().remove();
        $('.notifications a').first().addClass('active');
        $('#notif-group div').first().css('display', 'block');
    });

};


/* DATA TABLES */

function init_DataTables() {
    $.fn.dataTable.moment('ddd, MMMM Do, YYYY, h:mm:ss A');
    console.log('run_datatables');

    if (typeof ($.fn.DataTable) === 'undefined') {
        return;
    }
    console.log('init_DataTables');

    var handleDataTableButtons = function () {
        console.log('dataTable_buttons');
        $("#datatable-maps").DataTable({
            order: [[1, "asc"]],
            dom: "Bfrtip",
            buttons: [{
                extend: "copy",
                className: "badge badge-dark"
            }, {
                extend: "csv",
                className: "badge badge-dark"
            }, {
                extend: "excel",
                className: "badge badge-dark"
            }, {
                //     extend: "pdfHtml5",
                //     className: "badge badge-dark"
                // }, {
                extend: "print",
                className: "badge badge-dark"
            },],
            responsive: true,
            columnDefs: [
                {'width': '60px', targets: 'details', 'orderable': false}
            ],
        });
        $("#datatable-services").DataTable({
            order: [[0, "asc"]],
            dom: "Bfrtip",
            buttons: [{
                extend: "copy",
                className: "badge badge-dark"
            }, {
                extend: "csv",
                className: "badge badge-dark"
            }, {
                extend: "excel",
                className: "badge badge-dark"
            }, {
                //     extend: "pdfHtml5",
                //     className: "badge badge-dark"
                // }, {
                extend: "print",
                className: "badge badge-dark"
            },],
            responsive: true,
            columnDefs: [
                {'width': '60px', targets: 'details', 'orderable': false}
            ],
            orderCellsTop: true,
            initComplete: function () {
                this.api().columns([4]).every(function (d) {
                    var column = this;
                    var headingVal = $('#datatable-buttons1 th').eq([d]).text();
                    var select = $('<select><option value="">' + headingVal + ': All' + '</option></select>')
                        .appendTo($('#datatable-buttons1 thead tr:eq(1) th').eq(column.index()).empty())
                        .on('change', function () {
                            var val = $.fn.dataTable.util.escapeRegex(
                                $(this).val()
                            );

                            column
                                .search(val ? '^' + val + '$' : '', true, false)
                                .draw();
                        });

                    column.data().unique().sort().each(function (d, j) {
                        var val = $('<div/>').html(d).text();
                        select.append('<option value="' + val + '">' + val + '</option>')
                    });
                });
            }
        });
        $("#datatable-layers").DataTable({
            order: [[1, "asc"]],
            dom: "Bfrtip",
            buttons: [{
                extend: "copy",
                className: "badge badge-dark"
            }, {
                extend: "csv",
                className: "badge badge-dark"
            }, {
                extend: "excel",
                className: "badge badge-dark"
            }, {
                //     extend: "pdfHtml5",
                //     className: "badge badge-dark"
                // }, {
                extend: "print",
                className: "badge badge-dark"
            },],
            responsive: true,
            columnDefs: [
                {'width': '60px', targets: 'details', 'orderable': false}
            ],
        });
        $("#datatable-apps").DataTable({
            order: [[0, "asc"]],
            dom: "Bfrtip",
            buttons: [{
                extend: "copy",
                className: "badge badge-dark"
            }, {
                extend: "csv",
                className: "badge badge-dark"
            }, {
                extend: "excel",
                className: "badge badge-dark"
            }, {
                //     extend: "pdfHtml5",
                //     className: "badge badge-dark"
                // }, {
                extend: "print",
                className: "badge badge-dark"
            },],
            responsive: true,
            columnDefs: [
                {'width': '60px', targets: 'details', 'orderable': false}
            ],
        });
    };

    TableManageButtons = function () {
        "use strict";
        return {
            init: function () {
                handleDataTableButtons();
            }
        };
    }();

    $('#exampledatatable').DataTable({
        orderCellsTop: true,
        initComplete: function () {
            this.api().columns([1, 2, 3, 4]).every(function (d) {
                var column = this;
                var headingVal = $('#exampledatatable th').eq([d]).text();
                var select = $('<select><option value="">' + headingVal + ': All' + '</option></select>')
                    .appendTo($('#exampledatatable thead tr:eq(1) th').eq(column.index()).empty())
                    .on('change', function () {
                        var val = $.fn.dataTable.util.escapeRegex(
                            $(this).val()
                        );

                        column
                            .search(val ? '^' + val + '$' : '', true, false)
                            .draw();
                    });

                column.data().unique().sort().each(function (d, j) {
                    var val = $('<div/>').html(d).text();
                    select.append('<option value="' + val + '">' + val + '</option>')
                });
            });
        }
    });


    $('#datatable').DataTable();

    $('#datatable2').DataTable();

    $('#datatable3').DataTable();

    $('#datatable-keytable').DataTable({
        keys: true
    });

    $('#datatable-responsive').DataTable();

    $('#datatable-scroller').DataTable({
        ajax: "js/datatables/json/scroller-demo.json",
        deferRender: true,
        scrollY: 380,
        scrollCollapse: true,
        scroller: true
    });

    $('#datatable-fixed-header').DataTable({
        fixedHeader: true
    });


    TableManageButtons.init();

};

function init_Charts() {
    $(function () {
        var ctx = document.getElementById('line-chart');
        var data = JSON.parse(document.getElementById('chartdata').textContent);
        ctx.height = 80;
        var options = {
            scales: {
                yAxes: [{
                    scaleLabel: {
                        display: true,
                        labelString: 'RequestCount'
                    }
                }],
                xAxes: [{
                    type: 'time',
                    time: {
                        unit: 'day'
                    }
                }]
            },
            tooltips: {
                mode: 'index',
                intersect: true
            },
            plugins: {
                colorschemes: {
                    scheme: 'tableau.HueCircle19'
                }
            },
            responsive: true
        };
        new Chart(ctx, {
            type: 'line',
            maintainAspectRatio: false,
            data: data,
            options: options
        })
    });
}


$(document).ready(function () {

    init_sidebar();
    init_DataTables();
    init_PNotify();
    init_CustomNotification();
});
