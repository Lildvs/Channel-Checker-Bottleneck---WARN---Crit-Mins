declare module 'react-simple-maps' {
  import { FC, ReactNode, CSSProperties, MouseEvent } from 'react'

  export interface ComposableMapProps {
    projection?: string
    projectionConfig?: {
      scale?: number
      center?: [number, number]
      rotate?: [number, number, number]
    }
    width?: number
    height?: number
    style?: CSSProperties
    children?: ReactNode
  }

  export interface GeographiesProps {
    geography: string | object
    children: (data: { geographies: any[] }) => ReactNode
  }

  export interface GeographyProps {
    geography: any
    fill?: string
    stroke?: string
    strokeWidth?: number
    style?: {
      default?: CSSProperties
      hover?: CSSProperties
      pressed?: CSSProperties
    }
    onClick?: (event: MouseEvent) => void
    onMouseEnter?: (event: MouseEvent) => void
    onMouseLeave?: (event: MouseEvent) => void
    onMouseMove?: (event: MouseEvent) => void
  }

  export interface ZoomableGroupProps {
    center?: [number, number]
    zoom?: number
    minZoom?: number
    maxZoom?: number
    translateExtent?: [[number, number], [number, number]]
    onMoveStart?: (event: any) => void
    onMove?: (event: any) => void
    onMoveEnd?: (event: any) => void
    children?: ReactNode
  }

  export interface MarkerProps {
    coordinates: [number, number]
    style?: {
      default?: CSSProperties
      hover?: CSSProperties
      pressed?: CSSProperties
    }
    onClick?: (event: MouseEvent) => void
    onMouseEnter?: (event: MouseEvent) => void
    onMouseLeave?: (event: MouseEvent) => void
    children?: ReactNode
  }

  export interface LineProps {
    from: [number, number]
    to: [number, number]
    stroke?: string
    strokeWidth?: number
    strokeOpacity?: number
    strokeLinecap?: 'butt' | 'round' | 'square'
    strokeDasharray?: string
    className?: string
    style?: CSSProperties
  }

  export interface AnnotationProps {
    subject: [number, number]
    dx?: number
    dy?: number
    curve?: number
    connectorProps?: object
    children?: ReactNode
  }

  export interface GraticuleProps {
    stroke?: string
    strokeWidth?: number
    step?: [number, number]
  }

  export interface SphereProps {
    id?: string
    stroke?: string
    strokeWidth?: number
    fill?: string
  }

  export const ComposableMap: FC<ComposableMapProps>
  export const Geographies: FC<GeographiesProps>
  export const Geography: FC<GeographyProps>
  export const ZoomableGroup: FC<ZoomableGroupProps>
  export const Marker: FC<MarkerProps>
  export const Line: FC<LineProps>
  export const Annotation: FC<AnnotationProps>
  export const Graticule: FC<GraticuleProps>
  export const Sphere: FC<SphereProps>
}
