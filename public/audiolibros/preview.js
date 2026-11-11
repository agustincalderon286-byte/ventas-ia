(() => {
  'use strict';
  for (const audio of document.querySelectorAll('audio[data-audio-preview]')) {
    const source = audio.getAttribute('src');
    const status = document.createElement('p'); status.setAttribute('role', 'status'); audio.after(status);
    const controller = new TecAudioDelivery.Controller(audio, { onFailure: text => { status.textContent = text; } });
    controller.load(source).catch(error => { status.textContent = error.message; });
  }
})();
