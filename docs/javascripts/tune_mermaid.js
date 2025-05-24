//document.addEventListener('DOMContentLoaded', function() {


    const observer = new MutationObserver(function(mutations, observer) {

        console.log('xxxx');
        if (document.querySelectorAll('.node-labels').length > 0) {
            styleNodeLabels();
            observer.disconnect();  // Stop observing after first run
        }
    });
    
    observer.observe(document.body, {
        childList: true,
        subtree: true
    });


    // Function to style node labels
    function styleNodeLabels() {
        const nodeLabels = document.querySelectorAll('.node-labels');
        nodeLabels[0].style.fill = '#FF5733'
    }
//});
