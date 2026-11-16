// Public search snippets describe the edition actually available in TEC.
const searchNames = {
  'meditaciones-marco-aurelio': 'Meditaciones de Marco Aurelio',
  'genesis-rv1909': 'Génesis Reina-Valera 1909',
  'exodo-rv1909': 'Éxodo Reina-Valera 1909'
};
const descriptions = {
  'como-un-hombre-piensa': 'Escucha Como un hombre piensa, de James Allen, en español. Edición completa con lectura sincronizada y muestra gratis. Incluido en la membresía TEC.',
  'acres-de-diamantes': 'Acres de diamantes, de Russell H. Conwell: audiolibro en español sobre oportunidades e iniciativa. Prueba la muestra gratis y conoce la edición de TEC.',
  'el-arte-de-la-guerra': 'El arte de la guerra, de Sun Tzu: audiolibro completo en español, con trece capítulos y lectura sincronizada. Escucha una muestra gratis en TEC.',
  'genesis-rv1909': 'Escucha Génesis en español: los 50 capítulos de la Reina-Valera 1909, con lectura sincronizada. Muestra gratis; libro completo incluido en la membresía TEC.',
  'exodo-rv1909': 'Éxodo en audio en español: 40 capítulos completos de la Reina-Valera 1909 y lectura sincronizada. Escucha una muestra gratis y descubre la membresía TEC.',
  'ayudate': '¡Ayúdate!, de Samuel Smiles: audiolibro completo en español sobre esfuerzo y perseverancia. Con índice, lectura sincronizada y muestra gratis en TEC.',
  'la-ciencia-de-hacerse-rico': 'La ciencia de hacerse rico, de Wallace D. Wattles: audiolibro completo en español, con prefacio y 17 capítulos. Escucha la muestra gratis en TEC.',
  'el-kybalion': 'El Kybalión en español: audiolibro completo de Tres Iniciados sobre los siete principios herméticos. Con lectura sincronizada y muestra gratis en TEC.',
  'el-hombre-mas-rico-de-babilonia': 'El hombre más rico de Babilonia, de George S. Clason: audiolibro completo en español sobre ahorro y deudas. Escucha una muestra gratis en TEC.',
  'meditaciones-marco-aurelio': 'Meditaciones de Marco Aurelio: audiolibro en español con sus doce libros completos y lectura sincronizada. Prueba gratis la muestra de la edición TEC.',
  'mitos-y-heroes-de-grecia': 'Mitos y héroes de Grecia en español, de Thomas Bulfinch: capítulos 1–3 de ocho, con voces de personajes. Prueba gratis la muestra de esta entrega de TEC.',
  'viaje-al-oeste': 'Viaje al Oeste en español, de Wu Cheng’en: capítulos 1–5 de los 100 de la obra, con voces y lectura sincronizada. Escucha una muestra gratis en TEC.',
  'la-epopeya-de-gilgamesh': 'La epopeya de Gilgamesh: audiolibro en español de las doce tablillas del texto conservado, con voces y lectura sincronizada. Muestra gratis en TEC.',
  'don-quijote-de-la-mancha': 'Don Quijote de la Mancha en español actual: capítulos 1–10 de la primera parte, con voces y lectura sincronizada. Entrega parcial; muestra gratis en TEC.',
  'el-libro-de-los-cinco-anillos': 'El libro de los cinco anillos, de Miyamoto Musashi: audiolibro completo en español con prólogo y cinco rollos. Escucha una muestra gratis en TEC.',
  'la-psicologia-de-las-ventas': 'La psicología de las ventas, de William Walker Atkinson: diez capítulos completos en audio en español, con lectura sincronizada y muestra gratis en TEC.'
};
export const audiobookSearchTitle = book => `${searchNames[book.id] || book.title}: audiolibro en español | TEC`;
export const audiobookSearchDescription = book => descriptions[book.id] || `${book.title}, de ${book.author}: audiolibro en español. Conoce la edición disponible y su membresía en TEC.`;
