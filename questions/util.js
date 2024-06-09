async function init(){
    const plots = await fetch("plots.json").then((response) => {
        return response.json();
    });
    for (const [_, plot] of Object.entries(plots)) {
        initPlot(plot);
    }
}

function initPlot(plot) {
    const div = document.getElementById(plot["filename"]);
    if (div === null) {
        console.log("error, no div with id: " + plot["filename"]);
        return;
    }

    // This function now prepares the plot for rendering but doesn't render it immediately
    const renderPlot = () => {
        if (plot["type"] == "plotly_plot") {
            Plotly.newPlot(
                div,
                plot["traces"],
                plot["layout"],
                { staticPlot: screen.width < 850, responsive: true }
            );
        } else if (plot["type"] == "table") {
            div.innerHTML = '<div class="table-container">' + plot["innerHTML"] + '</div>';

            // TODO: add sorting with: "2020 ▼▲", "2020 ▼ ", "2020  ▲" Buttons
            // TODO: only make the first two columns sticky if the first column is "rank".

            // Calculate and set offset for each first thead th in rows
            var firstTheadTh = div.querySelector('thead th');
            var firstTheadThWidth = firstTheadTh.offsetWidth;
            var secondTheadThs = div.querySelectorAll('thead th:nth-child(2)');
            secondTheadThs.forEach(function(th) {
                th.style.left = firstTheadThWidth + 'px';
            });

            // Calculate and set offset for each first tbody th in rows
            div.querySelectorAll('tbody tr').forEach(function(row) {
                var firstThWidth = row.querySelector('th').offsetWidth;
                var secondTh = row.querySelector('th:nth-child(2)');
                if (secondTh) {
                    secondTh.style.left = firstThWidth + 'px';
                }
            });

        } else {
            console.error("unknown type: " + plot["type"]);
        }
    };

    // Use Intersection Observer to defer rendering until the div is visible
    const observer = new IntersectionObserver((entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                renderPlot();
                observer.unobserve(entry.target); // Stop observing the target once it has been rendered
            }
        });
    }, { threshold: 0.1 }); // Threshold - represents the percentage of visibility required before triggering

    observer.observe(div); // Start observing the div
}

function addToggle(buttonId, divId1, textButtonDiv1, divId2, textButtonDiv2){
    const div1 = document.getElementById(divId1);
    const div2 = document.getElementById(divId2);
    const modeButton = document.getElementById(buttonId);

    function toggle(){
        if (div2.style.display === 'none') {
            div2.style.display = '';
            div1.style.display = 'none';
            modeButton.textContent = textButtonDiv1;
        } else {
            div1.style.display = '';
            div2.style.display = 'none';
            modeButton.textContent = textButtonDiv2;
        }
    }

    modeButton.addEventListener('click', function() {
        toggle()
    });
    toggle()    
}

