#!/usr/bin/env python3
"""Build product-focused Coach images from the official INNOVE catalog PDF."""

from pathlib import Path

from PIL import Image, ImageOps
from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]
PDF = ROOT / "public/coach/resources/innove-catalog-usa-0226.pdf"
OUTPUT = ROOT / "public/coach/assets/catalog/innove/products"
SIZE = (800, 600)


def page_image(reader, page, index=0):
    return reader.pages[page - 1].images[index].image.convert("RGB")


def normalized_crop(image, box):
    if box is None:
        return image
    width, height = image.size
    left, top, right, bottom = box
    return image.crop((int(left * width), int(top * height), int(right * width), int(bottom * height)))


def save_product(image, name, box=None):
    cropped = normalized_crop(image, box)
    fitted = ImageOps.contain(cropped, SIZE, Image.Resampling.LANCZOS)
    canvas = Image.new("RGB", SIZE, "white")
    canvas.paste(fitted, ((SIZE[0] - fitted.width) // 2, (SIZE[1] - fitted.height) // 2))
    canvas.save(OUTPUT / f"{name}.webp", "WEBP", quality=88, method=6)


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    reader = PdfReader(str(PDF))

    # Crops from catalog compositions. Coordinates are normalized to the source image.
    crops = {
        "olla-1-5-cuartos": (8, 0, (0.00, 0.45, 0.42, 0.98)),
        "olla-2-cuartos": (8, 0, (0.42, 0.42, 0.86, 0.94)),
        "olla-3-cuartos": (9, 0, (0.00, 0.43, 0.37, 0.91)),
        "olla-4-cuartos": (8, 0, (0.49, 0.18, 0.86, 0.64)),
        "olla-6-cuartos": (9, 0, (0.20, 0.12, 0.58, 0.61)),
        "olla-8-cuartos": (7, 0, (0.00, 0.34, 0.43, 0.87)),
        "sarten-8-pulgadas": (8, 0, (0.28, 0.02, 0.63, 0.58)),
        "sarten-10-5-pulgadas": (9, 0, (0.48, 0.42, 0.82, 0.90)),
        "tapa-pequena": (8, 0, (0.70, 0.52, 0.96, 0.96)),
        "tapa-mediana": (8, 0, (0.00, 0.50, 0.42, 0.98)),
        "tapa-grande": (9, 0, (0.72, 0.35, 0.99, 0.93)),
        "tapa-alta": (6, 0, (0.26, 0.42, 0.58, 0.92)),
        "colador-pequeno": (11, 0, (0.00, 0.10, 0.55, 0.98)),
        "colador-grande": (11, 0, (0.46, 0.12, 1.00, 0.98)),
        "sarten-gourmet-8": (10, 0, (0.69, 0.42, 0.98, 0.89)),
        "sarten-gourmet-10": (10, 0, (0.39, 0.38, 0.76, 0.83)),
        "sarten-gourmet-12": (10, 0, (0.00, 0.32, 0.41, 0.76)),
        "easy-release-8": (12, 0, (0.21, 0.36, 0.50, 0.74)),
        "easy-release-10": (12, 0, (0.49, 0.24, 0.80, 0.78)),
        "easy-release-12": (12, 0, (0.13, 0.51, 0.48, 0.97)),
        "easy-release-utensilios": (12, 0, (0.78, 0.30, 0.98, 0.72)),
        "easy-release-olla-2": (13, 0, (0.27, 0.16, 0.55, 0.57)),
        "easy-release-olla-3": (13, 0, (0.00, 0.41, 0.48, 0.91)),
        "easy-release-olla-3-5": (13, 0, (0.49, 0.34, 0.86, 0.82)),
        "olla-presion-6": (15, 0, (0.10, 0.20, 0.50, 0.75)),
        "olla-presion-10": (15, 0, (0.49, 0.20, 0.91, 0.89)),
        "olla-12-cuartos": (16, 0, (0.00, 0.34, 0.39, 0.81)),
        "olla-20-cuartos": (16, 0, (0.33, 0.36, 0.69, 0.91)),
        "olla-30-cuartos": (16, 0, (0.61, 0.29, 0.94, 0.81)),
        "plancha-sencilla": (19, 0, (0.66, 0.26, 0.99, 0.94)),
        "plancha-redonda": (19, 0, (0.39, 0.20, 0.72, 0.66)),
        "plancha-doble": (19, 0, (0.00, 0.47, 0.51, 0.92)),
        "power-blender-max": (24, 0, (0.39, 0.04, 0.72, 0.88)),
        "jarra-tritan": (24, 0, (0.25, 0.23, 0.48, 0.72)),
        "maxcup": (24, 0, (0.68, 0.35, 0.84, 0.82)),
        "fresh-max": (24, 0, (0.84, 0.35, 0.98, 0.87)),
        "tazones-completos": (40, 0, None),
        "tazon-10-cuartos": (40, 0, (0.30, 0.21, 0.64, 0.75)),
        "tazones-dos": (40, 0, (0.54, 0.38, 0.85, 0.84)),
        "tazones-tres": (40, 0, (0.05, 0.31, 0.39, 0.78)),
        "tazon-5-cuartos": (40, 0, (0.58, 0.35, 0.89, 0.86)),
    }

    for name, (page, index, box) in crops.items():
        save_product(page_image(reader, page, index), name, box)

    # Images already separated in the PDF artwork.
    direct = {
        "paellera-10": (20, 0, (0.46, 0.00, 1.00, 1.00)),
        "paellera-14": (20, 0, (0.00, 0.00, 0.58, 1.00)),
        "cacerola": (20, 1, None),
        "perfect-pop": (21, 0, None),
        "hervidor-1": (21, 1, (0.00, 0.00, 0.58, 1.00)),
        "hervidor-medio": (21, 1, (0.46, 0.00, 1.00, 1.00)),
        "base-magnetica": (22, 2, None),
        "power-blender-go": (25, 0, None),
        "blender-go-taza-picadora": (25, 2, None),
        "blender-go-batidor-cepillo": (25, 3, None),
        "extractor-jugos": (26, 0, None),
        "exprimidor-jugos": (26, 2, None),
        "coladores-extractor": (26, 3, None),
        "cuchillos-cocina-5": (30, 0, None),
        "cuchillos-carne-4": (30, 1, None),
        "cuchillos-trinchar-4": (30, 2, None),
        "bloque-cuchillos": (30, 3, None),
        "cuchillo-pelar": (31, 0, None),
        "hacha-cocina": (31, 1, None),
        "cuchillos-churrasco": (31, 2, None),
        "santoku-3": (31, 3, None),
        "santoku-damascus": (31, 4, None),
        "tabla-bambu": (32, 0, (0.00, 0.00, 0.62, 1.00)),
        "tabla-bambu-pequena": (32, 0, (0.40, 0.00, 1.00, 1.00)),
        "cuchillo-multiusos": (32, 1, None),
        "afilador": (32, 2, None),
        "santoku-5": (32, 3, None),
        "expertea": (35, 0, None),
        "espresso-10": (35, 1, (0.00, 0.00, 0.63, 1.00)),
        "espresso-4": (35, 1, (0.47, 0.00, 1.00, 1.00)),
        "chocolatera": (36, 0, None),
        "tazas-pared-doble": (36, 1, None),
        "azucarera-jarro": (36, 2, None),
        "vasos-agua": (38, 0, None),
        "vasos-cortos": (38, 1, None),
        "copas-vino": (38, 2, None),
        "vasos-tequileros": (38, 3, None),
        "tazon-5-pared-doble": (43, 0, None),
        "tazon-mezclar-10": (43, 1, None),
        "utensilios-cocina-6": (44, 0, None),
        "recipiente-utensilios": (44, 1, None),
        "recipientes-almacenar": (44, 2, None),
        "copas-helado": (45, 1, None),
        "frescapure-3500": (48, 0, None),
        "frescapure-5500": (48, 1, None),
        "cartucho-frescapure": (48, 2, None),
        "filtro-ducha": (49, 0, None),
        "filtro-aire": (49, 1, None),
    }

    for name, (page, index, box) in direct.items():
        save_product(page_image(reader, page, index), name, box)

    gadgets = page_image(reader, 43, 2)
    gadget_crops = {
        "pelador-vertical": (0.10, 0.31, 0.19, 0.64),
        "pelador": (0.14, 0.27, 0.25, 0.59),
        "destapador": (0.20, 0.20, 0.30, 0.50),
        "espatula": (0.10, 0.55, 0.34, 0.88),
        "cortapizza": (0.36, 0.12, 0.55, 0.47),
        "machacador": (0.48, 0.29, 0.70, 0.64),
        "rallador": (0.67, 0.08, 0.82, 0.50),
        "espatula-helados": (0.46, 0.61, 0.67, 0.99),
        "espatula-silicona": (0.34, 0.46, 0.54, 0.94),
        "batidor-globo": (0.68, 0.38, 0.85, 0.87),
    }
    for name, box in gadget_crops.items():
        save_product(gadgets, name, box)

    print(f"Created {len(list(OUTPUT.glob('*.webp')))} product images in {OUTPUT}")


if __name__ == "__main__":
    main()
