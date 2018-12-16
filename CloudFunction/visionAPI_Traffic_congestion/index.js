exports.helloWorld = (req, res) => {
  // Imports the Google Cloud client library
  const vision = require('@google-cloud/vision');
  // Creates a client
  const client = new vision.ImageAnnotatorClient();

var msg;
  
client
  .labelDetection(`http://www.rms.nsw.gov.au/trafficreports/cameras/camera_images/parrard_parra.jpg?1544906206066`)
  .then(results => {
    const labels = results[0].labelAnnotations;
    console.log('Labels:');
    labels.forEach((label)=>{
      console.log(label.description);
      msg+=label.description;
      })
  })
  .catch(err => {
    console.error('ERROR:', err);
  });

  let message = req.query.message || req.body.message || 'Hello World!';
  res.status(200).send(msg);
};
