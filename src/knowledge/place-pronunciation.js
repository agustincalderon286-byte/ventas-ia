// Lugares y direcciones de Estados Unidos para la voz de Jorge (es-MX).
// Los vendedores viven en Estados Unidos: sus fichas traen ciudades y calles en inglés que una voz
// en español lee letra por letra o con sonidos del español. Aquí se escriben «como suenan», igual que
// los alias del catálogo. El chat conserva la escritura original; esto solo cambia lo que se pronuncia.
// La lista parte del área de Chicago, donde trabaja el piloto. Para otra zona se añaden sus ciudades.
export const placeAliases = {
  // Palabras de dirección
  'Avenue': 'Áveniu', 'Street': 'Estrít', 'Road': 'Róud', 'Boulevard': 'Búlevard', 'Highway': 'Jáiuei', 'Parkway': 'Párkuei',
  'Lane': 'Léin', 'Court': 'Cort', 'Drive': 'Dráiv', 'Heights': 'Jáits', 'Island': 'Áiland', 'Lake': 'Léik', 'Grove': 'Gróuv',
  'Hills': 'Jils', 'Village': 'Vílich', 'Forest': 'Fórest', 'Ridge': 'Rich', 'River': 'Ríver',
  'North': 'Nort', 'South': 'Sáut', 'East': 'Íst', 'West': 'Uést', 'Illinois': 'Ilinói', 'Wisconsin': 'Uiscónsin',
  // Ciudades del área de Chicago y el noroeste de Indiana
  'Blue Island': 'Blú Áiland', 'Chicago Heights': 'Chicago Jáits', 'Cicero': 'Sísero', 'Berwyn': 'Béruin', 'Oak Lawn': 'Óuk Lon',
  'Oak Park': 'Óuk Park', 'Oak Forest': 'Óuk Fórest', 'Joliet': 'Yóliet', 'Elgin': 'Élyin', 'Waukegan': 'Uokígan',
  'Des Plaines': 'Des Pléins', 'Naperville': 'Néipervil', 'Schaumburg': 'Shómberg', 'Harvey': 'Járvi', 'Calumet City': 'Cáliumet Síti',
  'Calumet Park': 'Cáliumet Park', 'Calumet': 'Cáliumet', 'Melrose Park': 'Mélrous Park', 'Franklin Park': 'Fránklin Park',
  'Bolingbrook': 'Bólinbruk', 'Hammond': 'Jámond', 'East Chicago': 'Íst Chicago', 'West Chicago': 'Uést Chicago', 'Evanston': 'Évanston',
  'Burbank': 'Bérbank', 'Bridgeview': 'Brích Viú', 'Summit': 'Sámit', 'Stickney': 'Stíkni', 'Dolton': 'Dólton', 'Riverdale': 'Ríverdeil',
  'Midlothian': 'Midlódian', 'Alsip': 'Ólsip', 'Markham': 'Márkam', 'Hazel Crest': 'Jéisel Crest', 'Homewood': 'Jóumwud',
  'Lansing': 'Lánsing', 'South Holland': 'Sáut Jóland', 'Tinley Park': 'Tínli Park', 'Orland Park': 'Órland Park', 'Palos Hills': 'Pálos Jils',
  'Hickory Hills': 'Jíkori Jils', 'Romeoville': 'Rómiovil', 'Wheeling': 'Uíling', 'Mundelein': 'Mándelain', 'Round Lake': 'Ráund Léik',
  'Carpentersville': 'Cárpentersvil', 'Hanover Park': 'Jánover Park', 'Streamwood': 'Strímwud', 'Addison': 'Ádison', 'Bensenville': 'Bénsenvil',
  'Northlake': 'Nórtleik', 'Stone Park': 'Stóun Park', 'Maywood': 'Méiwud', 'Bellwood': 'Bélwud', 'Hillside': 'Jílsaid',
  'Whiting': 'Uáiting', 'Highland': 'Jáiland', 'Milwaukee': 'Miluóki', 'Rockford': 'Rókford', 'Kankakee': 'Kankakí',
  // Calles frecuentes de Chicago
  'Western': 'Uéstern', 'Halsted': 'Jólsted', 'Ashland': 'Áshland', 'Pulaski': 'Puláski', 'Kedzie': 'Kédsi', 'Harlem': 'Járlem',
  'Cermak': 'Sérmak', 'Archer': 'Árcher', 'Roosevelt': 'Róusevelt', 'Fullerton': 'Fúlerton', 'Lawrence': 'Lórens', 'Irving Park': 'Érving Park'
};

const streetWords = {Ave:'Avenue',St:'Street',Rd:'Road',Blvd:'Boulevard',Hwy:'Highway',Pkwy:'Parkway',Ln:'Lane',Ct:'Court',Dr:'Drive'};
const states = {IL:'Illinois',IN:'Indiana',WI:'Wisconsin',MI:'Michigan',TX:'Texas',CA:'California',FL:'Florida',NY:'Nueva York'};
// Una abreviatura de calle va después de un número o de un nombre propio: «127th St», «Western Ave».
// Así «el Dr. García» y «en St. Charles» se quedan como están.
const street = /(?<=(?:\d(?:st|nd|rd|th)?|(?<![\p{L}\p{N}])\p{Lu}[\p{L}'’]+) )(Ave|St|Rd|Blvd|Hwy|Pkwy|Ln|Ct|Dr)(?![\p{L}\p{N}])/gu;
const compass = {N:'North',S:'South',E:'East',W:'West'};

// Prepara una dirección para leerla en voz alta: el rumbo y las abreviaturas completos, y los números de calle sin «th».
export function expandAddressWords(text) {
  return text.replace(/(?<=\b\d{1,6} )([NSEW])\.?(?= [\p{L}\p{N}])/gu,(found,letter)=>compass[letter])
    // «Dr» seguido de un nombre propio es un doctor, no una calle: «Llamar Dr. García».
    .replace(street,(found,word,offset,whole)=>word==='Dr' && /^\.? \p{Lu}/u.test(whole.slice(offset+2)) ? found : streetWords[word])
    .replace(/\b(\d{1,5})(?:st|nd|rd|th)\b/g,'$1')
    // El estado, después de una coma o justo antes del código postal.
    .replace(/(?<=, ?)(IL|IN|WI|MI|TX|CA|FL|NY)(?![\p{L}\p{N}])|(?<= )(IL|IN|WI|MI|TX|CA|FL|NY)(?= \d{5}\b)/g,code=>states[code]);
}
