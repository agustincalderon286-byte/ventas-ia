from pathlib import Path
import tempfile

import numpy as np
import trimesh
from pygltflib import GLTF2, Material, PbrMetallicRoughness
from pxr import Gf, Sdf, Usd, UsdGeom, UsdShade, UsdUtils


INCH = 25.4


def inch(value):
    return value * INCH


def vec(x, y, z):
    return np.array([float(x), float(y), float(z)], dtype=np.float64)


def build_cylinder(radius, start, end, sections):
    return trimesh.creation.cylinder(
        radius=radius,
        segment=np.array([start, end], dtype=np.float64),
        sections=sections,
    )


def build_box(size_x, size_y, size_z, center):
    mesh = trimesh.creation.box(extents=[size_x, size_y, size_z])
    mesh.apply_translation(center)
    return mesh


def build_handrail_mesh(sections=48):
    rise = inch(7)
    tread = inch(11)
    steps = 4
    landing = inch(48)

    total_rise = rise * steps
    stair_run = tread * steps

    rail_height = inch(36)
    mid_rail_offset = inch(16)
    post_radius = inch(1.0)
    rail_radius = inch(0.78)
    baluster_radius = inch(0.36)
    plate_size = inch(4.0)
    plate_thickness = inch(0.25)

    bottom_base = vec(0, 0, 0)
    top_base = vec(stair_run, 0, total_rise)
    landing_end_base = vec(stair_run + landing, 0, total_rise)

    bottom_top = bottom_base + vec(0, 0, rail_height)
    top_top = top_base + vec(0, 0, rail_height)
    landing_end_top = landing_end_base + vec(0, 0, rail_height)

    bottom_mid = bottom_base + vec(0, 0, rail_height - mid_rail_offset)
    top_mid = top_base + vec(0, 0, rail_height - mid_rail_offset)
    landing_end_mid = landing_end_base + vec(0, 0, rail_height - mid_rail_offset)

    parts = []

    # Base plates
    for base in (bottom_base, top_base, landing_end_base):
        plate_center = base + vec(0, 0, plate_thickness / 2)
        parts.append(build_box(plate_size, plate_size, plate_thickness, plate_center))

    # Posts
    parts.append(build_cylinder(post_radius, bottom_base, bottom_top, sections))
    parts.append(build_cylinder(post_radius, top_base, top_top, sections))
    parts.append(build_cylinder(post_radius, landing_end_base, landing_end_top, sections))

    # Rails
    parts.append(build_cylinder(rail_radius, bottom_top, top_top, sections))
    parts.append(build_cylinder(rail_radius, top_top, landing_end_top, sections))
    parts.append(build_cylinder(rail_radius * 0.74, bottom_mid, top_mid, sections))
    parts.append(build_cylinder(rail_radius * 0.74, top_mid, landing_end_mid, sections))

    # Balusters on stair section
    for index in range(1, 5):
        t = index / 5
        upper = bottom_top + (top_top - bottom_top) * t
        lower = bottom_mid + (top_mid - bottom_mid) * t
        parts.append(build_cylinder(baluster_radius, lower, upper, max(20, sections // 2)))

    # Balusters on landing
    for index in range(1, 5):
        t = index / 5
        upper = top_top + (landing_end_top - top_top) * t
        lower = top_mid + (landing_end_mid - top_mid) * t
        parts.append(build_cylinder(baluster_radius, lower, upper, max(20, sections // 2)))

    mesh = trimesh.util.concatenate(parts)
    mesh.process(validate=True)
    mesh.merge_vertices()

    # Center laterally so it feels better in viewer, keep floor at z=0
    bounds = mesh.bounds
    center_x = (bounds[0][0] + bounds[1][0]) / 2
    center_y = (bounds[0][1] + bounds[1][1]) / 2
    mesh.apply_translation([-center_x, -center_y, -bounds[0][2]])
    return mesh


def add_stainless_black_material(glb_path):
    gltf = GLTF2().load_binary(str(glb_path))
    material = Material(
        name="ArchitecturalBlackSteel",
        pbrMetallicRoughness=PbrMetallicRoughness(
            baseColorFactor=[0.12, 0.13, 0.14, 1.0],
            metallicFactor=0.96,
            roughnessFactor=0.18,
        ),
        alphaMode="OPAQUE",
        doubleSided=False,
    )
    gltf.materials = [material]
    for mesh in gltf.meshes or []:
        for primitive in mesh.primitives or []:
            primitive.material = 0
    gltf.save_binary(str(glb_path))


def export_usdz(mesh, usda_path, usdz_path):
    stage = Usd.Stage.CreateNew(str(usda_path))
    UsdGeom.SetStageUpAxis(stage, UsdGeom.Tokens.z)
    UsdGeom.SetStageMetersPerUnit(stage, 0.001)

    root = UsdGeom.Xform.Define(stage, "/Root")
    stage.SetDefaultPrim(root.GetPrim())

    usd_mesh = UsdGeom.Mesh.Define(stage, "/Root/Handrail")
    usd_mesh.CreatePointsAttr([Gf.Vec3f(float(x), float(y), float(z)) for x, y, z in mesh.vertices])
    usd_mesh.CreateFaceVertexCountsAttr([3] * len(mesh.faces))
    usd_mesh.CreateFaceVertexIndicesAttr([int(i) for face in mesh.faces for i in face])
    usd_mesh.CreateSubdivisionSchemeAttr("none")
    usd_mesh.CreateNormalsAttr([Gf.Vec3f(float(x), float(y), float(z)) for x, y, z in mesh.vertex_normals])
    usd_mesh.SetNormalsInterpolation(UsdGeom.Tokens.vertex)

    UsdGeom.Scope.Define(stage, "/Root/Looks")
    material = UsdShade.Material.Define(stage, "/Root/Looks/ArchitecturalBlackSteel")
    shader = UsdShade.Shader.Define(stage, "/Root/Looks/ArchitecturalBlackSteel/PreviewSurface")
    shader.CreateIdAttr("UsdPreviewSurface")
    shader.CreateInput("diffuseColor", Sdf.ValueTypeNames.Color3f).Set(Gf.Vec3f(0.12, 0.13, 0.14))
    shader.CreateInput("metallic", Sdf.ValueTypeNames.Float).Set(1.0)
    shader.CreateInput("roughness", Sdf.ValueTypeNames.Float).Set(0.2)
    shader.CreateInput("ior", Sdf.ValueTypeNames.Float).Set(1.5)
    material.CreateSurfaceOutput().ConnectToSource(shader.ConnectableAPI(), "surface")
    UsdShade.MaterialBindingAPI(usd_mesh).Bind(material)

    stage.GetRootLayer().Save()
    UsdUtils.CreateNewARKitUsdzPackage(Sdf.AssetPath(str(usda_path)), str(usdz_path))


def export_preview_svg(svg_path):
    rise = inch(7)
    tread = inch(11)
    steps = 4
    landing = inch(48)
    rail_height = inch(36)
    mid_offset = inch(16)
    total_rise = rise * steps
    stair_run = tread * steps
    landing_end = stair_run + landing
    width = landing_end + inch(16)
    height = total_rise + rail_height + inch(20)
    margin = inch(8)
    scale = 0.34

    def sx(x):
        return margin + x * scale

    def sz(z):
        return height - margin - z * scale

    lines = []
    x = 0
    z = 0
    stair_points = [(x, z)]
    for _ in range(steps):
        x += tread
        stair_points.append((x, z))
        z += rise
        stair_points.append((x, z))
    stair_points.append((landing_end, z))
    lines.append((stair_points, "#26323f", 8))
    lines.append(([(0, rail_height), (stair_run, total_rise + rail_height)], "#0f172a", 10))
    lines.append(([(stair_run, total_rise + rail_height), (landing_end, total_rise + rail_height)], "#0f172a", 10))
    lines.append(([(0, rail_height - mid_offset), (stair_run, total_rise + rail_height - mid_offset)], "#475569", 6))
    lines.append(([(stair_run, total_rise + rail_height - mid_offset), (landing_end, total_rise + rail_height - mid_offset)], "#475569", 6))
    for points in (
        [(0, 0), (0, rail_height)],
        [(stair_run, total_rise), (stair_run, total_rise + rail_height)],
        [(landing_end, total_rise), (landing_end, total_rise + rail_height)],
    ):
        lines.append((points, "#111827", 12))

    for index in range(1, 5):
        t = index / 5
        x_pos = stair_run * t
        lines.append(([(x_pos, rail_height - mid_offset + total_rise * t), (x_pos, rail_height + total_rise * t)], "#334155", 4))
    for index in range(1, 5):
        t = index / 5
        x_pos = stair_run + landing * t
        lines.append(([(x_pos, total_rise + rail_height - mid_offset), (x_pos, total_rise + rail_height)], "#334155", 4))

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{int(width * scale + margin * 2)}" height="{int(height)}" viewBox="0 0 {int(width * scale + margin * 2)} {int(height)}">',
        '<rect width="100%" height="100%" fill="#f8fafc"/>',
        f'<text x="{margin}" y="42" font-family="Arial, sans-serif" font-size="24" font-weight="700" fill="#0f172a">Concepto 3D: pasamanos para 4 escalones + landing</text>',
        f'<text x="{margin}" y="70" font-family="Arial, sans-serif" font-size="14" fill="#475569">Base visual para web AR y presentacion con cliente</text>',
    ]
    for points, color, stroke in lines:
        encoded = " ".join(f"{sx(px):.1f},{sz(pz):.1f}" for px, pz in points)
        svg.append(
            f'<polyline points="{encoded}" fill="none" stroke="{color}" stroke-width="{stroke}" stroke-linecap="round" stroke-linejoin="round"/>'
        )
    svg.append("</svg>")
    svg_path.write_text("\n".join(svg))


def main():
    asset_dir = Path("public/prototipo-handrail-ar/assets")
    asset_dir.mkdir(parents=True, exist_ok=True)

    medium_mesh = build_handrail_mesh(sections=48)
    low_mesh = build_handrail_mesh(sections=24)

    medium_glb = asset_dir / "handrail-4step-landing-architectural.glb"
    usdz_path = asset_dir / "handrail-4step-landing-architectural.usdz"
    preview_svg = asset_dir / "handrail-4step-landing-preview.svg"

    medium_mesh.export(medium_glb)
    add_stainless_black_material(medium_glb)

    with tempfile.TemporaryDirectory() as tmp_dir:
        low_ply = Path(tmp_dir) / "handrail-4step-landing-architectural-low.ply"
        usda_path = Path(tmp_dir) / "handrail-4step-landing-architectural.usda"
        low_mesh.export(low_ply)
        export_usdz(low_mesh, usda_path, usdz_path)
    export_preview_svg(preview_svg)

    print(medium_glb)
    print(usdz_path)
    print(preview_svg)


if __name__ == "__main__":
    main()
