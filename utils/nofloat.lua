function Image(img)
  img.attributes["unnumbered"] = true
  img.attributes["placement"] = "H"
  return img
end

return {
  Image = Image
}

