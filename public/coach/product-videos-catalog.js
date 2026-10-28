/* Official, manually reviewed links. No video files or provider credentials stored by TEC.
   Sources: official Royal Prestige channels and Hy Cite publisher-provided embeds, reviewed 2026-10-28. Keep code mappings explicit;
   never infer a model from a loose term such as "licuadora", "olla" or a spare part. */
(function (root) {
  'use strict';
  const videos = [
  {
    "product": "Power Blender Max",
    "title": "¿Cómo usar tu Royal Prestige® Power Blender Max?",
    "id": "CWVjdd84sZA",
    "duration": "0:50",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=CWVjdd84sZA",
    "family": "blender-max"
  },
  {
    "product": "Power Blender Go",
    "title": "Cómo usar tu Royal Prestige® Power Blender Go",
    "id": "mN2JvoB9aeo",
    "duration": "1:42",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=mN2JvoB9aeo",
    "family": "blender-go"
  },
  {
    "product": "Power Blender Go Plus",
    "title": "Cómo usar y cuidar tu Royal Prestige® Power Blender Go Plus",
    "id": "Naoe05OwjW8",
    "duration": "3:42",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=Naoe05OwjW8",
    "family": "blender-go-plus"
  },
  {
    "product": "Extractor de jugos",
    "title": "Cómo Usar tu Royal Prestige® Juicer",
    "id": "bPa7N0R-FGg",
    "duration": "",
    "type": "Uso y cuidados",
    "category": "Electrodomésticos",
    "url": "https://www.youtube.com/watch?v=bPa7N0R-FGg",
    "family": "juicer"
  },
  {
    "product": "NOVEL",
    "title": "Sistemas de Cocina Royal Prestige® NOVEL™ Uso y Cuidados",
    "id": "4zKG8m3jhkA",
    "duration": "1:22",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=4zKG8m3jhkA",
    "family": "novel"
  },
  {
    "product": "INNOVE",
    "title": "Antes de Usar tu Royal Prestige® INNOVE™ Por Primera Vez",
    "id": "5G86YsDe6-4",
    "duration": "1:22",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=5G86YsDe6-4",
    "family": "innove"
  },
  {
    "product": "Sistema de 5 capas",
    "title": "5 Capas Uso y Cuidado",
    "id": "aa3c1fX83N8",
    "duration": "1:21",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=aa3c1fX83N8",
    "family": "5-ply"
  },
  {
    "product": "Olla de presión",
    "title": "Royal Prestige® Pressure Cooker Uso y Cuidados",
    "id": "t6LCgx98lVU",
    "duration": "3:00",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=t6LCgx98lVU",
    "family": "pressure"
  },
  {
    "product": "Olla de presión · presentación",
    "title": "Royal Prestige® Pressure Cooker evoluciona",
    "id": "g0VqtkEeIcM",
    "duration": "1:02",
    "type": "Presentación",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=g0VqtkEeIcM",
    "family": "pressure"
  },
  {
    "product": "Deluxe Easy Release",
    "title": "Como Cuidar tu Royal Prestige® Deluxe Easy Release",
    "id": "gIRieCFeMQc",
    "duration": "0:54",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=gIRieCFeMQc",
    "family": "easy-release"
  },
  {
    "product": "Deluxe Easy Release · presentación",
    "title": "Royal Prestige® Deluxe Easy Release Cookware",
    "id": "m7tI3WypyVQ",
    "duration": "1:14",
    "type": "Presentación",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=m7tI3WypyVQ",
    "family": "easy-release"
  },
  {
    "product": "MultiPan",
    "title": "Conoce el Royal Prestige® MultiPan",
    "id": "i4FhdX12EpI",
    "duration": "0:57",
    "type": "Presentación",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=i4FhdX12EpI",
    "family": "multipan"
  },
  {
    "product": "Chocolatera",
    "title": "Cómo Usar tu Royal Prestige® Chocolatera",
    "id": "9Qp3YyHIL4U",
    "duration": "0:51",
    "type": "Uso y cuidados",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=9Qp3YyHIL4U",
    "family": "chocolatera"
  },
  {
    "product": "Paellera",
    "title": "Cómo Usar tu Royal Prestige® Paella Pan",
    "id": "1Y8tRXCYYeE",
    "duration": "0:50",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=1Y8tRXCYYeE",
    "family": "paella"
  },
  {
    "product": "Pavera ovalada",
    "title": "Pavera Ovalada Uso y Cuidados U&C",
    "id": "oagV3XP4ANA",
    "duration": "1:13",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=oagV3XP4ANA",
    "family": "roaster"
  },
  {
    "product": "Warmer Pro",
    "title": "Cómo usar y cuidar tu Royal Prestige® Warmer Pro",
    "id": "7mWnwsxppwc",
    "duration": "1:22",
    "type": "Uso y cuidados",
    "category": "Electrodomésticos",
    "url": "https://www.youtube.com/watch?v=7mWnwsxppwc",
    "family": "warmer"
  },
  {
    "product": "Filtro de aire",
    "title": "Royal Prestige® Air Filtration System Uso y Cuidados",
    "id": "G39m2Xuekgc",
    "duration": "2:37",
    "type": "Uso y cuidados",
    "category": "Filtración",
    "url": "https://www.youtube.com/watch?v=G39m2Xuekgc",
    "family": "air"
  },
  {
    "product": "FrescaFlow · beneficios",
    "title": "Conoce los Beneficios de tu Royal Prestige® FrescaFlow",
    "id": "N5UzL3Lgj38",
    "duration": "1:43",
    "type": "Presentación",
    "category": "Filtración",
    "url": "https://www.youtube.com/watch?v=N5UzL3Lgj38",
    "family": "frescaflow"
  },
  {
    "product": "FrescaFlow · presentación",
    "title": "¡Conoce el Royal Prestige® FrescaFlow!",
    "id": "psqkTxfZ3Pw",
    "duration": "1:20",
    "type": "Presentación",
    "category": "Filtración",
    "url": "https://www.youtube.com/watch?v=psqkTxfZ3Pw",
    "family": "frescaflow"
  },
  {
    "product": "FrescaFlow · cartuchos",
    "title": "Cómo y Cuándo Reemplazar los Cartuchos de Royal Prestige® FrescaFlow",
    "id": "yCRIsyi62GU",
    "duration": "2:49",
    "type": "Uso y cuidados",
    "category": "Filtración",
    "url": "https://www.youtube.com/watch?v=yCRIsyi62GU",
    "family": "frescaflow"
  },
  {
    "product": "ExperTea",
    "title": "Cómo Usar y Cuidar tu Royal Prestige® ExperTea",
    "id": "Pfl3ZcOPajE",
    "duration": "1:04",
    "type": "Uso y cuidados",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=Pfl3ZcOPajE",
    "family": "expertea"
  },
  {
    "product": "Cuchillos",
    "title": "Cómo usar y cuidar tu Royal Prestige® Cutlery",
    "id": "VFrJLn1MeD4",
    "duration": "0:40",
    "type": "Uso y cuidados",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=VFrJLn1MeD4",
    "family": "cutlery"
  },
  {
    "product": "Tazones para mezclar",
    "title": "Chef Omar te enseña sobre los nuevos Royal Prestige® Mixing Bowls",
    "id": "LggddajOKlA",
    "duration": "1:19",
    "type": "Presentación",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=LggddajOKlA",
    "family": "bowls"
  },
  {
    "product": "Barista",
    "title": "Te Presentamos el Nuevo Royal Prestige® Barista",
    "id": "iy4tTfyv_wY",
    "duration": "0:43",
    "type": "Presentación",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=iy4tTfyv_wY",
    "family": "barista"
  },
  {
    "product": "Barista",
    "title": "Cómo Usar y Cuidar Tu Royal Prestige® Barista",
    "id": "we0PGQPAw48",
    "duration": "",
    "type": "Uso y cuidados",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=we0PGQPAw48",
    "family": "barista"
  },
  {
    "product": "BaristArt Kit",
    "title": "Te Presentamos el Nuevo Royal Prestige® BaristArt Kit",
    "id": "1bpewZi0r5w",
    "duration": "1:01",
    "type": "Presentación",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=1bpewZi0r5w",
    "family": "baristart"
  },
  {
    "product": "Precision Cook",
    "title": "Royal Prestige® Precision Cook",
    "id": "OroIxlvUVTA",
    "duration": "1:25",
    "type": "Presentación",
    "category": "Electrodomésticos",
    "url": "https://www.youtube.com/watch?v=OroIxlvUVTA",
    "family": "precision-cook"
  },
  {
    "product": "Precision Cook",
    "title": "Cómo Usar tu Royal Prestige® Precision Cook",
    "id": "1J5rF6naZ9o",
    "duration": "3:22",
    "type": "Uso y cuidados",
    "category": "Electrodomésticos",
    "url": "https://www.youtube.com/watch?v=1J5rF6naZ9o",
    "family": "precision-cook"
  },
  {
    "product": "Royal Espresso · 4 y 10 tazas",
    "title": "Royal Prestige® Espresso",
    "id": "ACweIGyBcsg",
    "duration": "1:02",
    "type": "Presentación",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=ACweIGyBcsg",
    "family": "espresso"
  },
  {
    "product": "Royal Espresso · 4 y 10 tazas",
    "title": "Cómo Usar tu Royal Prestige® Espresso",
    "id": "0dn4OHRnKhA",
    "duration": "1:10",
    "type": "Uso y cuidados",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=0dn4OHRnKhA",
    "family": "espresso"
  },
  {
    "product": "Perfect Pop",
    "title": "Royal Prestige® Perfect Pop",
    "id": "AYscfmVhG1s",
    "duration": "0:42",
    "type": "Presentación",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=AYscfmVhG1s",
    "family": "perfect-pop"
  },
  {
    "product": "Fresh Max",
    "title": "Royal Prestige® Fresh Max",
    "id": "vp1LX7r7pyw",
    "duration": "1:04",
    "type": "Presentación",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=vp1LX7r7pyw",
    "family": "fresh-max"
  },
  {
    "product": "Fresh Max",
    "title": "Cómo Usar tu Royal Prestige® Fresh Max",
    "id": "gD0q9oD6knE",
    "duration": "1:07",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=gD0q9oD6knE",
    "family": "fresh-max"
  },
  {
    "product": "Max Cup",
    "title": "Cómo Usar tu Royal Prestige® Max Cup",
    "id": "GwBDGFtjJno",
    "duration": "0:40",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=GwBDGFtjJno",
    "family": "max-cup"
  },
  {
    "product": "Jarra de Tritan para Power Blender Max",
    "title": "¿Cómo usar tu Royal Prestige® Power Blender Max con Jarra de Tritan?",
    "id": "zXhP18jvRK8",
    "duration": "0:50",
    "type": "Uso y cuidados",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=zXhP18jvRK8",
    "family": "tritan-jar"
  },
  {
    "product": "Cacerola Royal Prestige",
    "title": "Royal Prestige® Casserole",
    "id": "qvdt-Q8eQJ8",
    "duration": "1:07",
    "type": "Presentación",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=qvdt-Q8eQJ8",
    "family": "casserole"
  },
  {
    "product": "Cacerola Royal Prestige",
    "title": "Cómo Usar tu Royal Prestige® Casserole",
    "id": "3Ys_VtMeR-4",
    "duration": "",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=3Ys_VtMeR-4",
    "family": "casserole"
  },
  {
    "product": "Bloque de cuchillos All-in-One",
    "title": "Cómo Usar y Cuidar tu Royal Prestige® All-In-One Knife Block",
    "id": "CuTE0_XE66U",
    "duration": "1:41",
    "type": "Uso y cuidados",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=CuTE0_XE66U",
    "family": "knife-block"
  },
  {
    "product": "Cuchillos para churrasco",
    "title": "¿Sabes cómo cuidar tus cuchillos Royal Prestige®?",
    "id": "mfQS_GM1mf4",
    "duration": "0:57",
    "type": "Uso y cuidados",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=mfQS_GM1mf4",
    "family": "churrasco-knives"
  },
  {
    "product": "Juego de cuchillos para carne · 4 piezas",
    "title": "Te Presentamos el Nuevo Royal Prestige® Steak Knife Set",
    "id": "p668z2hXy2c",
    "duration": "0:17",
    "type": "Presentación",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=p668z2hXy2c",
    "family": "steak-knives"
  },
  {
    "product": "Juego de 4 piezas para rebanar",
    "title": "Te Presentamos el Nuevo Royal Prestige® Carving Knife Set",
    "id": "UNd9ca53PW0",
    "duration": "0:22",
    "type": "Presentación",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=UNd9ca53PW0",
    "family": "carving-knives"
  },
  {
    "product": "Cuidados de cuchillos Santoku, para carne y para rebanar",
    "title": "Cómo Cuidar Tus Cuchillos de Royal Prestige®",
    "id": "OoMRj50udSY",
    "duration": "1:05",
    "type": "Uso y cuidados",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=OoMRj50udSY",
    "family": "knife-care"
  },
  {
    "product": "Sartenes Gourmet",
    "title": "Aprende a Cocinar con Chef Omar y las Sartenes Gourmet de Royal Prestige®",
    "id": "vwvQFg9pKyo",
    "duration": "15:04",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=vwvQFg9pKyo",
    "family": "gourmet-skillets"
  },

  {
    "product": "FrescaPure 5500 · sobre el mostrador",
    "title": "FrescaPure 5500 · sobre el mostrador · uso y cuidados",
    "id": "hycite-5500-ct",
    "provider": "hycite",
    "embedUrl": "https://hycite.widen.net/view/video/7lgy7w356t/Royal-Prestige%C2%AE-Frescapure-5500CT_Use-Care-Video_SPA.mp4?x.portal_shortcode_generated=kygv5pel&x.collection_sharename=xhrvreeq&x.app=portals",
    "thumbnail": "/coach/assets/catalog/innove/products/frescapure-5500.webp",
    "duration": "1:45",
    "type": "Uso y cuidados",
    "category": "Filtración",
    "url": "https://hycite.widen.net/view/video/7lgy7w356t/Royal-Prestige%C2%AE-Frescapure-5500CT_Use-Care-Video_SPA.mp4?x.portal_shortcode_generated=kygv5pel&x.collection_sharename=xhrvreeq&x.app=portals",
    "family": "frescapure-5500-ct"
  },
  {
    "product": "FrescaPure 5500 · bajo el mostrador",
    "title": "FrescaPure 5500 · bajo el mostrador · uso y cuidados",
    "id": "hycite-5500-uc",
    "provider": "hycite",
    "embedUrl": "https://hycite.widen.net/view/video/fpgs5gkwlv/Royal-Prestige%C2%AE-Frescapure-5500UC_Use-Care-Video_SPA.mp4?x.portal_shortcode_generated=kygv5pel&x.collection_sharename=xhrvreeq&x.app=portals",
    "thumbnail": "",
    "duration": "1:41",
    "type": "Uso y cuidados",
    "category": "Filtración",
    "url": "https://hycite.widen.net/view/video/fpgs5gkwlv/Royal-Prestige%C2%AE-Frescapure-5500UC_Use-Care-Video_SPA.mp4?x.portal_shortcode_generated=kygv5pel&x.collection_sharename=xhrvreeq&x.app=portals",
    "family": "frescapure-5500-uc"
  },
  {
    "product": "FrescaPure 3500",
    "title": "FrescaPure 3500 · uso y cuidados",
    "id": "hycite-3500",
    "provider": "hycite",
    "embedUrl": "https://hycite.widen.net/view/video/kn6ykc3sxg/Royal-Prestige%C2%AE-Frescapure-3500_Use-Care-Video_ESP.mp4?x.portal_shortcode_generated=pjvcbrd7&x.collection_sharename=3w1psnag&x.app=portals",
    "thumbnail": "/coach/assets/catalog/innove/products/frescapure-3500.webp",
    "duration": "1:20",
    "type": "Uso y cuidados",
    "category": "Filtración",
    "url": "https://hycite.widen.net/view/video/kn6ykc3sxg/Royal-Prestige%C2%AE-Frescapure-3500_Use-Care-Video_ESP.mp4?x.portal_shortcode_generated=pjvcbrd7&x.collection_sharename=3w1psnag&x.app=portals",
    "family": "frescapure-3500"
  },
  {
    "product": "Royal Wok INNOVE",
    "title": "Royal Wok INNOVE · uso y cuidados",
    "id": "hycite-wok-innove",
    "provider": "hycite",
    "embedUrl": "https://hycite.widen.net/view/video/odnkhmbjnq/INNOVE-Wok_Use-Care-Video_YT_SPA.mp4?x.portal_shortcode_generated=pastjwqh&x.collection_sharename=tkvcc7ot&x.app=portals",
    "thumbnail": "/coach/assets/catalog/innove/catalogo-innove-pagina-04.webp",
    "duration": "1:08",
    "type": "Uso y cuidados",
    "category": "Ollas y sartenes",
    "url": "https://hycite.widen.net/view/video/odnkhmbjnq/INNOVE-Wok_Use-Care-Video_YT_SPA.mp4?x.portal_shortcode_generated=pastjwqh&x.collection_sharename=tkvcc7ot&x.app=portals",
    "family": "wok-innove"
  },
  {
    "product": "Afilador de cuchillos",
    "title": "Royal Prestige® Afilador de Cuchillos - Uso y Cuidados",
    "id": "gfIyzRlbXaI",
    "duration": "0:41",
    "type": "Uso y cuidados",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=gfIyzRlbXaI",
    "family": "butterfly-sharpener"
  },
  {
    "product": "Máquina de ensaladas",
    "title": "Máquina de Ensaladas de Royal Prestige · SALAD MACHINE",
    "id": "y1sz5QIV5oc",
    "duration": "0:32",
    "type": "Presentación",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=y1sz5QIV5oc",
    "family": "salad-machine"
  }
,

  {
    "product": "MultiPan",
    "title": "Muslos de Pollo Braseados",
    "id": "REj6w1WIzcw",
    "duration": "0:48",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=REj6w1WIzcw",
    "family": "recipe-multipan",
    "requirements": "MultiPan."
  },
  {
    "product": "MultiPan",
    "title": "Tallarines Salteados de Verduras",
    "id": "467WPWSALpg",
    "duration": "1:25",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=467WPWSALpg",
    "family": "recipe-multipan",
    "requirements": "MultiPan."
  },
  {
    "product": "MultiPan",
    "title": "Pastel de Chocolate Espresso",
    "id": "a79DV18g4Eo",
    "duration": "1:57",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=a79DV18g4Eo",
    "family": "recipe-multipan",
    "requirements": "MultiPan."
  },
  {
    "product": "Olla de presión de 6 litros",
    "title": "Costillas Estofadas con Puré de Papa",
    "id": "R57vXzfvnOI",
    "duration": "2:30",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=R57vXzfvnOI",
    "family": "recipe-pressure-6",
    "requirements": "Olla de presión de 6 litros."
  },
  {
    "product": "Olla de presión de 10 litros",
    "title": "Birria de Res",
    "id": "iH3BuWHXIGQ",
    "duration": "2:52",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=iH3BuWHXIGQ",
    "family": "recipe-pressure-10",
    "requirements": "Olla de presión de 10 litros."
  },
  {
    "product": "Power Blender Max",
    "title": "Agua Chile Verde",
    "id": "5tCU3oBmKss",
    "duration": "0:51",
    "type": "Recetas",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=5tCU3oBmKss",
    "family": "recipe-blender-max",
    "requirements": "Power Blender Max."
  },
  {
    "product": "Power Blender Max",
    "title": "Dip de Queso Fresco Batido con Verduras de Verano",
    "id": "DhcwmfZ-jVU",
    "duration": "1:01",
    "type": "Recetas",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=DhcwmfZ-jVU",
    "family": "recipe-blender-max",
    "requirements": "Power Blender Max."
  },
  {
    "product": "Power Blender Go + recipiente picador Go Plus",
    "title": "Sofrito",
    "id": "HDhDwSA8628",
    "duration": "0:51",
    "type": "Recetas",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=HDhDwSA8628",
    "family": "recipe-blender-go",
    "requirements": "Power Blender Go con recipiente picador Go Plus. Requiere ese accesorio."
  },
  {
    "product": "Power Blender Go + jarra Go Plus + Easy Release de 10 pulgadas",
    "title": "Tortitas de avena y plátano",
    "id": "wp7e0izJSOs",
    "duration": "0:57",
    "type": "Recetas",
    "category": "Licuadoras",
    "url": "https://www.youtube.com/watch?v=wp7e0izJSOs",
    "family": "recipe-blender-go",
    "requirements": "Power Blender Go con jarra Go Plus y sartén Deluxe Easy Release de 10 pulgadas."
  },
  {
    "product": "Sartén Deluxe Easy Release",
    "title": "Camarones empanizados",
    "id": "S9e6fkvYX4s",
    "duration": "2:21",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=S9e6fkvYX4s",
    "family": "recipe-easy-release",
    "requirements": "Sartén Deluxe Easy Release. Ajusta las cantidades a la capacidad de tu sartén."
  },
  {
    "product": "Sartén Deluxe Easy Release de 10 pulgadas",
    "title": "Ensalada de verduras con nueces y crumble de parmesano",
    "id": "_p1vb_ffosM",
    "duration": "1:40",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=_p1vb_ffosM",
    "family": "recipe-easy-release-10",
    "requirements": "Sartén Deluxe Easy Release de 10 pulgadas."
  },
  {
    "product": "Barista",
    "title": "Café de Olla",
    "id": "Fg1T6lbrlbg",
    "duration": "0:51",
    "type": "Recetas",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=Fg1T6lbrlbg",
    "family": "recipe-barista",
    "requirements": "Barista."
  },
  {
    "product": "Barista",
    "title": "Café Latte con Miel y Coco",
    "id": "D5c5W1UBCKQ",
    "duration": "0:56",
    "type": "Recetas",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=D5c5W1UBCKQ",
    "family": "recipe-barista",
    "requirements": "Barista."
  },
  {
    "product": "Barista",
    "title": "Café Helado con galletas",
    "id": "rxSm2HLkSuE",
    "duration": "0:50",
    "type": "Recetas",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=rxSm2HLkSuE",
    "family": "recipe-barista",
    "requirements": "Barista."
  },
  {
    "product": "Chocolatera",
    "title": "Chocolate Caliente",
    "id": "yn6iKMIyq2s",
    "duration": "0:31",
    "type": "Recetas",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=yn6iKMIyq2s",
    "family": "recipe-chocolatera",
    "requirements": "Chocolatera."
  },
  {
    "product": "Chocolatera",
    "title": "Chocolate Caliente de Naranja",
    "id": "p1e3gYyCrIA",
    "duration": "1:13",
    "type": "Recetas",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=p1e3gYyCrIA",
    "family": "recipe-chocolatera",
    "requirements": "Chocolatera."
  },
  {
    "product": "Extractor de jugos Juicer",
    "title": "Jugo de manzana verde y pepino",
    "id": "vreJJ16s1fk",
    "duration": "0:31",
    "type": "Recetas",
    "category": "Electrodomésticos",
    "url": "https://www.youtube.com/watch?v=vreJJ16s1fk",
    "family": "recipe-juicer",
    "requirements": "Extractor de jugos Juicer."
  },
  {
    "product": "Extractor de jugos Juicer",
    "title": "Shots de Manzana Verde, Espinacas y Naranja, Jengibre y Cúrcuma",
    "id": "eTUSfc2BrOg",
    "duration": "1:07",
    "type": "Recetas",
    "category": "Electrodomésticos",
    "url": "https://www.youtube.com/watch?v=eTUSfc2BrOg",
    "family": "recipe-juicer",
    "requirements": "Extractor de jugos Juicer."
  },
  {
    "product": "ExperTea",
    "title": "Té de Limón con Miel y Jengibre",
    "id": "KRdQe2vzNiI",
    "duration": "1:00",
    "type": "Recetas",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=KRdQe2vzNiI",
    "family": "recipe-expertea",
    "requirements": "ExperTea."
  },
  {
    "product": "ExperTea",
    "title": "Té con Manzana",
    "id": "9t1whiNGz98",
    "duration": "1:03",
    "type": "Recetas",
    "category": "Café y té",
    "url": "https://www.youtube.com/watch?v=9t1whiNGz98",
    "family": "recipe-expertea",
    "requirements": "ExperTea."
  },
  {
    "product": "Máquina de ensaladas + sartén de 10.5 pulgadas",
    "title": "Lasaña Vegetariana",
    "id": "y0E9xvDDhjo",
    "duration": "1:01",
    "type": "Recetas",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=y0E9xvDDhjo",
    "family": "recipe-salad-machine",
    "requirements": "Máquina de ensaladas y un sartén de 10.5 pulgadas."
  },
  {
    "product": "Sartén NOVEL de 10.5 pulgadas",
    "title": "Ensalada de Salmón a la Plancha y en Escamas",
    "id": "h0I_lgJ1758",
    "duration": "1:20",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=h0I_lgJ1758",
    "family": "recipe-novel-10-5",
    "requirements": "Sartén NOVEL de 10.5 pulgadas."
  },
  {
    "product": "Sartén NOVEL de 10.5 pulgadas",
    "title": "Ensalada de pechuga de pollo",
    "id": "xdAOOykr3Nk",
    "duration": "1:17",
    "type": "Recetas",
    "category": "Ollas y sartenes",
    "url": "https://www.youtube.com/watch?v=xdAOOykr3Nk",
    "family": "recipe-novel-10-5",
    "requirements": "Sartén NOVEL de 10.5 pulgadas."
  },
  {
    "product": "Mixing Bowls con accesorios de rallado y corte",
    "title": "Ensalada Agitada",
    "id": "ULpGq-7tooA",
    "duration": "0:36",
    "type": "Recetas",
    "category": "Accesorios",
    "url": "https://www.youtube.com/watch?v=ULpGq-7tooA",
    "family": "recipe-mixing-bowls",
    "requirements": "Mixing Bowls con sus accesorios de rallado y corte. No corresponde al juego antiguo de tres tazones."
  }

];
  const families = [
  {
    "id": "blender-max",
    "label": "Power Blender Max",
    "codes": [
      "ES0082"
    ],
    "videoIds": [
      "CWVjdd84sZA"
    ]
  },
  {
    "id": "blender-go",
    "label": "Power Blender Go",
    "codes": [
      "ES4200"
    ],
    "videoIds": [
      "mN2JvoB9aeo"
    ]
  },
  {
    "id": "blender-go-plus",
    "label": "Power Blender Go Plus",
    "codes": [],
    "videoIds": [
      "Naoe05OwjW8"
    ]
  },
  {
    "id": "juicer",
    "label": "Extractor de jugos",
    "codes": [
      "JU0038"
    ],
    "videoIds": [
      "bPa7N0R-FGg"
    ]
  },
  {
    "id": "novel",
    "label": "NOVEL",
    "codes": [
      "CO9893",
      "CO9563",
      "CO9573",
      "CO9883",
      "CO9554",
      "CO9550",
      "CO9555",
      "CO9560",
      "CO9565",
      "CO9570",
      "CO9575",
      "CO9580",
      "CO9590",
      "CO9598",
      "CO9660",
      "CO9675",
      "CO9144"
    ],
    "videoIds": [
      "4zKG8m3jhkA"
    ]
  },
  {
    "id": "innove",
    "label": "INNOVE",
    "codes": [
      "CO9846",
      "CO9251",
      "CO9265",
      "CO9232",
      "CO9234",
      "CO9256",
      "CO9261",
      "CO9267",
      "CO9268",
      "CO9269",
      "CO9315",
      "CO9317",
      "CO9220",
      "CO9219",
      "CO9218"
    ],
    "videoIds": [
      "5G86YsDe6-4"
    ]
  },
  {
    "id": "5-ply",
    "label": "Sistema de 5 capas",
    "codes": [
      "CO7014",
      "CO7024",
      "CO7034",
      "CO7044",
      "CO8072",
      "CO8070",
      "CO9687",
      "CO9686",
      "CO9685"
    ],
    "videoIds": [
      "aa3c1fX83N8"
    ]
  },
  {
    "id": "pressure",
    "label": "Olla de presión",
    "codes": [
      "CO1253",
      "CO1258"
    ],
    "videoIds": [
      "t6LCgx98lVU",
      "g0VqtkEeIcM"
    ]
  },
  {
    "id": "easy-release",
    "label": "Deluxe Easy Release",
    "codes": [
      "CO9740",
      "CO9736",
      "CO9738",
      "CO9739",
      "CO9780",
      "CO9777",
      "CO9778",
      "CO9779"
    ],
    "videoIds": [
      "gIRieCFeMQc",
      "m7tI3WypyVQ"
    ]
  },
  {
    "id": "multipan",
    "label": "MultiPan",
    "codes": [
      "CO4911"
    ],
    "videoIds": [
      "i4FhdX12EpI"
    ]
  },
  {
    "id": "chocolatera",
    "label": "Chocolatera",
    "codes": [
      "CO0101"
    ],
    "videoIds": [
      "9Qp3YyHIL4U"
    ]
  },
  {
    "id": "paella",
    "label": "Paellera",
    "codes": [
      "CO9598",
      "CO9315",
      "CO9317",
      "CO8072",
      "CO8070"
    ],
    "videoIds": [
      "1Y8tRXCYYeE"
    ]
  },
  {
    "id": "roaster",
    "label": "Pavera ovalada",
    "codes": [
      "CO6589"
    ],
    "videoIds": [
      "oagV3XP4ANA"
    ]
  },
  {
    "id": "warmer",
    "label": "Warmer Pro",
    "codes": [
      "PE4000"
    ],
    "videoIds": [
      "7mWnwsxppwc"
    ]
  },
  {
    "id": "air",
    "label": "Filtro de aire",
    "codes": [
      "AP2801"
    ],
    "videoIds": [
      "G39m2Xuekgc"
    ]
  },
  {
    "id": "frescaflow",
    "label": "FrescaFlow",
    "codes": [
      "WF1200"
    ],
    "videoIds": [
      "N5UzL3Lgj38",
      "psqkTxfZ3Pw",
      "yCRIsyi62GU"
    ]
  },
  {
    "id": "expertea",
    "label": "ExperTea",
    "codes": [
      "CO1001"
    ],
    "videoIds": [
      "Pfl3ZcOPajE"
    ]
  },
  {
    "id": "cutlery",
    "label": "Cuchillos",
    "codes": [
      "CU0931",
      "CU0800",
      "CU0810",
      "CU814",
      "CU0825"
    ],
    "videoIds": [
      "VFrJLn1MeD4"
    ]
  },
  {
    "id": "bowls",
    "label": "Tazones para mezclar",
    "codes": [
      "CO4900"
    ],
    "videoIds": [
      "LggddajOKlA"
    ]
  },
  {
    "id": "barista",
    "label": "Barista",
    "codes": [
      "CO2106"
    ],
    "videoIds": [
      "iy4tTfyv_wY",
      "we0PGQPAw48"
    ]
  },
  {
    "id": "baristart",
    "label": "BaristArt Kit",
    "codes": [
      "PR2134"
    ],
    "videoIds": [
      "1bpewZi0r5w"
    ]
  },
  {
    "id": "precision-cook",
    "label": "Precision Cook",
    "codes": [
      "ES0071"
    ],
    "videoIds": [
      "OroIxlvUVTA",
      "1J5rF6naZ9o"
    ]
  },
  {
    "id": "espresso",
    "label": "Royal Espresso",
    "codes": [
      "PR2128",
      "PR2129"
    ],
    "videoIds": [
      "ACweIGyBcsg",
      "0dn4OHRnKhA"
    ]
  },
  {
    "id": "perfect-pop",
    "label": "Perfect Pop",
    "codes": [
      "PR0025"
    ],
    "videoIds": [
      "AYscfmVhG1s"
    ]
  },
  {
    "id": "fresh-max",
    "label": "Fresh Max",
    "codes": [
      "PE0040"
    ],
    "videoIds": [
      "vp1LX7r7pyw",
      "gD0q9oD6knE"
    ]
  },
  {
    "id": "max-cup",
    "label": "Max Cup",
    "codes": [
      "PE0051"
    ],
    "videoIds": [
      "GwBDGFtjJno"
    ]
  },
  {
    "id": "tritan-jar",
    "label": "Jarra de Tritan",
    "codes": [
      "PE0050"
    ],
    "videoIds": [
      "zXhP18jvRK8"
    ]
  },
  {
    "id": "casserole",
    "label": "Cacerola",
    "codes": [
      "PR2124"
    ],
    "videoIds": [
      "qvdt-Q8eQJ8",
      "3Ys_VtMeR-4"
    ]
  },
  {
    "id": "knife-block",
    "label": "Bloque de cuchillos All-in-One",
    "codes": [
      "CU0056"
    ],
    "videoIds": [
      "CuTE0_XE66U"
    ]
  },
  {
    "id": "churrasco-knives",
    "label": "Cuchillos para churrasco",
    "codes": [
      "CU0740"
    ],
    "videoIds": [
      "mfQS_GM1mf4"
    ]
  },
  {
    "id": "steak-knives",
    "label": "Cuchillos para carne",
    "codes": [
      "CU0820"
    ],
    "videoIds": [
      "p668z2hXy2c"
    ]
  },
  {
    "id": "carving-knives",
    "label": "Juego de 4 piezas para rebanar",
    "codes": [
      "CU0815"
    ],
    "videoIds": [
      "UNd9ca53PW0"
    ]
  },
  {
    "id": "knife-care",
    "label": "Cuidados de cuchillos",
    "codes": [
      "CU0820",
      "CU0815",
      "SP0296",
      "SP0297"
    ],
    "videoIds": [
      "OoMRj50udSY"
    ]
  },
  {
    "id": "gourmet-skillets",
    "label": "Sartenes Gourmet",
    "codes": [
      "CO9655",
      "CO9630",
      "CO9640",
      "CO9650"
    ],
    "videoIds": [
      "vwvQFg9pKyo"
    ]
  },
  {
    "id": "frescaflow-cartridges",
    "label": "Cartuchos FrescaFlow",
    "codes": [
      "PR2002",
      "PR2003",
      "PR2004"
    ],
    "videoIds": [
      "yCRIsyi62GU"
    ]
  },

  {
    "id": "frescapure-5500-ct",
    "label": "FrescaPure 5500 · sobre el mostrador",
    "codes": [
      "WF0482"
    ],
    "videoIds": [
      "hycite-5500-ct"
    ]
  },
  {
    "id": "frescapure-5500-uc",
    "label": "FrescaPure 5500 · bajo el mostrador",
    "codes": [
      "WF0485"
    ],
    "videoIds": [
      "hycite-5500-uc"
    ]
  },
  {
    "id": "frescapure-3500",
    "label": "FrescaPure 3500",
    "codes": [
      "WF0419"
    ],
    "videoIds": [
      "hycite-3500"
    ]
  },
  {
    "id": "wok-innove",
    "label": "Royal Wok INNOVE",
    "codes": [
      "CO9222"
    ],
    "videoIds": [
      "hycite-wok-innove"
    ]
  },
  {
    "id": "butterfly-sharpener",
    "label": "Afilador de cuchillos",
    "codes": [
      "PR1841"
    ],
    "videoIds": [
      "gfIyzRlbXaI"
    ]
  },
  {
    "id": "salad-machine",
    "label": "Máquina de ensaladas",
    "codes": [
      "PR2614"
    ],
    "videoIds": [
      "y1sz5QIV5oc"
    ]
  }
,

  {
    "id": "recipe-multipan",
    "label": "Recetas MultiPan",
    "codes": [
      "CO4911"
    ],
    "videoIds": [
      "REj6w1WIzcw",
      "467WPWSALpg",
      "a79DV18g4Eo"
    ]
  },
  {
    "id": "recipe-pressure-6",
    "label": "Recetas olla de presión de 6 litros",
    "codes": [
      "CO1253"
    ],
    "videoIds": [
      "R57vXzfvnOI"
    ]
  },
  {
    "id": "recipe-pressure-10",
    "label": "Recetas olla de presión de 10 litros",
    "codes": [
      "CO1258"
    ],
    "videoIds": [
      "iH3BuWHXIGQ"
    ]
  },
  {
    "id": "recipe-blender-max",
    "label": "Recetas Power Blender Max",
    "codes": [
      "ES0082"
    ],
    "videoIds": [
      "5tCU3oBmKss",
      "DhcwmfZ-jVU"
    ]
  },
  {
    "id": "recipe-blender-go",
    "label": "Recetas Power Blender Go con accesorios",
    "codes": [
      "ES4200"
    ],
    "videoIds": [
      "HDhDwSA8628",
      "wp7e0izJSOs"
    ]
  },
  {
    "id": "recipe-easy-release",
    "label": "Recetas sartenes Deluxe Easy Release",
    "codes": [
      "CO9740",
      "CO9736",
      "CO9738",
      "CO9739"
    ],
    "videoIds": [
      "S9e6fkvYX4s"
    ]
  },
  {
    "id": "recipe-easy-release-10",
    "label": "Recetas Deluxe Easy Release de 10 pulgadas",
    "codes": [
      "CO9740",
      "CO9738"
    ],
    "videoIds": [
      "wp7e0izJSOs",
      "_p1vb_ffosM"
    ]
  },
  {
    "id": "recipe-barista",
    "label": "Recetas Barista",
    "codes": [
      "CO2106"
    ],
    "videoIds": [
      "Fg1T6lbrlbg",
      "D5c5W1UBCKQ",
      "rxSm2HLkSuE"
    ]
  },
  {
    "id": "recipe-chocolatera",
    "label": "Recetas Chocolatera",
    "codes": [
      "CO0101"
    ],
    "videoIds": [
      "yn6iKMIyq2s",
      "p1e3gYyCrIA"
    ]
  },
  {
    "id": "recipe-juicer",
    "label": "Recetas Juicer",
    "codes": [
      "JU0038"
    ],
    "videoIds": [
      "vreJJ16s1fk",
      "eTUSfc2BrOg"
    ]
  },
  {
    "id": "recipe-expertea",
    "label": "Recetas ExperTea",
    "codes": [
      "CO1001"
    ],
    "videoIds": [
      "KRdQe2vzNiI",
      "9t1whiNGz98"
    ]
  },
  {
    "id": "recipe-salad-machine",
    "label": "Recetas máquina de ensaladas",
    "codes": [
      "PR2614"
    ],
    "videoIds": [
      "y0E9xvDDhjo"
    ]
  },
  {
    "id": "recipe-novel-10-5",
    "label": "Recetas NOVEL de 10.5 pulgadas",
    "codes": [
      "CO9590",
      "CO9893",
      "CO9563",
      "CO9883",
      "CO9554"
    ],
    "videoIds": [
      "h0I_lgJ1758",
      "xdAOOykr3Nk"
    ]
  },
  {
    "id": "recipe-mixing-bowls",
    "label": "Recetas tazones para mezclar y rallar",
    "codes": [
      "CO4900"
    ],
    "videoIds": [
      "ULpGq-7tooA"
    ]
  }

];
  const normalize = value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  const byId = new Map(videos.map(video => [video.id, Object.freeze(video)]));
  function forProduct(product) {
    if (!product) return [];
    const code = String(product.codigo_producto || product.code || '').trim().toUpperCase();
    const name = normalize(product.nombre_producto || product.name);
    // The same price-sheet code appears in two lines. A named line is required.
    const selected = code === 'CO9630' ? (name.includes('innove') ? ['innove'] : name.includes('gourmet') ? ['gourmet-skillets'] : [])
      : code ? families.filter(family => family.codes.includes(code)).map(family => family.id)
      : families.filter(family => normalize(family.label) === name || normalize('Royal Prestige ' + family.label) === name).map(family => family.id);
    // Shared care videos are linked explicitly, not inherited from a whole line.
    const videoIds = new Set(families.filter(family => selected.includes(family.id)).flatMap(family => family.videoIds));
    return videos.filter(video => videoIds.has(video.id));
  }
  function filterVideos(items, { query = '', category = '', type = '' } = {}) {
    const term = normalize(query);
    return items.filter(video => (!type || video.type === type) && (!category || video.category === category)
      && (!term || normalize(`${video.product} ${video.title} ${video.type} ${video.requirements || ''} ${video.category === 'Licuadoras' ? 'licuadora batidora' : ''}`).includes(term)));
  }
  // Resolve only catalog IDs: caller-provided URLs can never become iframe sources.
  function embedUrl(id) {
    const video = byId.get(id);
    if (!video) return '';
    if (!video.provider || video.provider === 'youtube') {
      return /^[a-zA-Z0-9_-]{11}$/.test(video.id)
        ? `https://www.youtube.com/embed/${video.id}?hl=es&playsinline=1&rel=0&autoplay=0` : '';
    }
    if (video.provider !== 'hycite') return '';
    try {
      const url = new URL(video.embedUrl);
      return url.protocol === 'https:' && url.hostname === 'hycite.widen.net' && !url.username && !url.password && !url.port
        && /^\/view\/video\/[a-z0-9]{10}\/[^/]+\.mp4$/.test(url.pathname) ? url.href : '';
    } catch { return ''; }
  }
  function thumbnailUrl(id) {
    const video = byId.get(id);
    if (!video || !embedUrl(id)) return '';
    if (video.provider === 'hycite') {
      return /^\/coach\/assets\/catalog\/innove\/(?:products\/)?[a-z0-9-]+\.webp$/.test(video.thumbnail || '') ? video.thumbnail : '';
    }
    return `https://i.ytimg.com/vi/${video.id}/hqdefault.jpg`;
  }
  root.TecProductVideoCatalog = Object.freeze({
    version: '20261028-recipes1', videos: Object.freeze(videos), families: Object.freeze(families),
    forProduct, filterVideos, byId: id => byId.get(id) || null, embedUrl, thumbnailUrl, normalize
  });
})(globalThis);
